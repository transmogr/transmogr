package crypto

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type staticProvider struct {
	snapshot Snapshot
	err      error
}

func (p staticProvider) Load(context.Context) (Snapshot, error) {
	if p.err != nil {
		return Snapshot{}, p.err
	}

	return cloneSnapshot(p.snapshot), nil
}

func TestNewCachedManagerLoadsInitialSnapshot(t *testing.T) {
	t.Parallel()

	manager, err := NewCachedManager(context.Background(), staticProvider{
		snapshot: Snapshot{
			ActiveKeyID: "current",
			KeysByID: map[string][]byte{
				"current": []byte("0123456789abcdef0123456789abcdef"),
			},
		},
	}, KeyManagerConfig{RefreshInterval: time.Minute, RetryAttempts: 1, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatalf("new cached manager: %v", err)
	}

	key, err := manager.GetKey(context.Background(), "current")
	if err != nil {
		t.Fatalf("get key: %v", err)
	}
	if string(key) != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("unexpected key: %q", key)
	}
}

func TestNewCachedManagerRejectsInvalidInitialSnapshot(t *testing.T) {
	t.Parallel()

	_, err := NewCachedManager(context.Background(), staticProvider{
		snapshot: Snapshot{
			ActiveKeyID: "missing",
			KeysByID: map[string][]byte{
				"current": []byte("0123456789abcdef0123456789abcdef"),
			},
		},
	}, KeyManagerConfig{RefreshInterval: time.Minute, RetryAttempts: 1, RetryDelay: time.Millisecond})
	if err == nil {
		t.Fatal("expected invalid snapshot error")
	}
}

func TestNewCachedManagerRejectsMissingRequiredKey(t *testing.T) {
	t.Parallel()

	_, err := NewCachedManager(context.Background(), staticProvider{
		snapshot: Snapshot{
			ActiveKeyID: "current",
			KeysByID: map[string][]byte{
				"current": []byte("0123456789abcdef0123456789abcdef"),
			},
		},
	}, KeyManagerConfig{
		RefreshInterval: time.Minute,
		RetryAttempts:   1,
		RetryDelay:      time.Millisecond,
		RequiredKeyIDs:  []string{"email-key"},
	})
	if err == nil {
		t.Fatal("expected missing required key error")
	}
}

func TestCachedManagerClonesReturnedKeys(t *testing.T) {
	t.Parallel()

	manager, err := NewCachedManager(context.Background(), staticProvider{
		snapshot: Snapshot{
			ActiveKeyID: "current",
			KeysByID: map[string][]byte{
				"current": []byte("0123456789abcdef0123456789abcdef"),
			},
		},
	}, KeyManagerConfig{RefreshInterval: time.Minute, RetryAttempts: 1, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatalf("new cached manager: %v", err)
	}

	key, err := manager.GetKey(context.Background(), "current")
	if err != nil {
		t.Fatalf("get key: %v", err)
	}
	key[0] = 'x'

	next, err := manager.GetKey(context.Background(), "current")
	if err != nil {
		t.Fatalf("get key again: %v", err)
	}
	if string(next) != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("unexpected stored key mutation: %q", next)
	}
}

func TestNewCachedManagerReturnsProviderError(t *testing.T) {
	t.Parallel()

	_, err := NewCachedManager(context.Background(), staticProvider{
		err: errors.New("boom"),
	}, KeyManagerConfig{RefreshInterval: time.Minute, RetryAttempts: 1, RetryDelay: time.Millisecond})
	if err == nil {
		t.Fatal("expected provider error")
	}
}

type sequenceProvider struct {
	mu        sync.Mutex
	responses []providerResponse
}

type providerResponse struct {
	snapshot Snapshot
	err      error
}

func (p *sequenceProvider) Load(context.Context) (Snapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.responses) == 0 {
		return Snapshot{}, errors.New("no responses configured")
	}

	next := p.responses[0]
	p.responses = p.responses[1:]
	if next.err != nil {
		return Snapshot{}, next.err
	}

	return cloneSnapshot(next.snapshot), nil
}

func TestNewCachedManagerRetriesInitialLoad(t *testing.T) {
	t.Parallel()

	provider := &sequenceProvider{
		responses: []providerResponse{
			{err: errors.New("boom-1")},
			{err: errors.New("boom-2")},
			{
				snapshot: Snapshot{
					ActiveKeyID: "current",
					KeysByID: map[string][]byte{
						"current": []byte("0123456789abcdef0123456789abcdef"),
					},
				},
			},
		},
	}

	manager, err := NewCachedManager(context.Background(), provider, KeyManagerConfig{
		RefreshInterval: time.Minute,
		RetryAttempts:   3,
		RetryDelay:      time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new cached manager: %v", err)
	}
	if err := manager.Ready(context.Background()); err != nil {
		t.Fatalf("expected ready manager, got %v", err)
	}
}

func TestCachedManagerReadyFailsAfterRefreshRetriesExhausted(t *testing.T) {
	t.Parallel()

	provider := &sequenceProvider{
		responses: []providerResponse{
			{
				snapshot: Snapshot{
					ActiveKeyID: "current",
					KeysByID: map[string][]byte{
						"current": []byte("0123456789abcdef0123456789abcdef"),
					},
				},
			},
			{err: errors.New("boom-1")},
			{err: errors.New("boom-2")},
		},
	}

	manager, err := NewCachedManager(context.Background(), provider, KeyManagerConfig{
		RefreshInterval: time.Minute,
		RetryAttempts:   2,
		RetryDelay:      time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new cached manager: %v", err)
	}

	if err := manager.refresh(context.Background()); err == nil {
		t.Fatal("expected refresh error")
	}
	if err := manager.Ready(context.Background()); err == nil {
		t.Fatal("expected manager to become not ready after failed refresh")
	}
}
