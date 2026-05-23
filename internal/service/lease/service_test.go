package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/transmogr/transmogr/internal/models"
)

func TestRunWithLease_RunsAndReleasesLease(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	service := NewService(store, Config{
		Kind:          "stream",
		Region:        "local",
		PeerRegion:    "peer",
		OwnerID:       "pod-1",
		TTL:           time.Second,
		RenewInterval: time.Hour,
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lease := models.Lease{
		Kind:       "stream",
		Region:     "local",
		PeerRegion: "peer",
		OwnerID:    "pod-1",
		Generation: 3,
	}
	store.EXPECT().
		TryStateAcquireLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", time.Second).
		Return(lease, true, nil)
	store.EXPECT().
		ReleaseStateLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", int64(3)).
		Return(true, nil)

	calls := 0
	err := service.RunWithLease(ctx, func(context.Context) error {
		calls++
		cancel()
		return nil
	})
	if err != nil {
		t.Fatalf("RunWithLease() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("fn calls = %d, want 1", calls)
	}
}

func TestRunWithLease_RetriesUntilLeaseAcquired(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	service := NewService(store, Config{
		Kind:          "stream",
		Region:        "local",
		PeerRegion:    "peer",
		OwnerID:       "pod-1",
		TTL:           time.Second,
		RenewInterval: 5 * time.Millisecond,
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := store.EXPECT().
		TryStateAcquireLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", time.Second).
		Return(models.Lease{}, false, nil)
	lease := models.Lease{
		Kind:       "stream",
		Region:     "local",
		PeerRegion: "peer",
		OwnerID:    "pod-1",
		Generation: 1,
	}
	store.EXPECT().
		TryStateAcquireLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", time.Second).
		Return(lease, true, nil).
		After(first)
	store.EXPECT().
		ReleaseStateLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", int64(1)).
		Return(true, nil)

	calls := 0
	err := service.RunWithLease(ctx, func(context.Context) error {
		calls++
		cancel()
		return nil
	})
	if err != nil {
		t.Fatalf("RunWithLease() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("fn calls = %d, want 1", calls)
	}
}

func TestRunWithLease_RetriesFunctionErrorAndReleasesLease(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	service := NewService(store, Config{
		Kind:          "stream",
		Region:        "local",
		PeerRegion:    "peer",
		OwnerID:       "pod-1",
		TTL:           time.Second,
		RenewInterval: 5 * time.Millisecond,
	}, nil)
	wantErr := errors.New("boom")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstLease := models.Lease{
		Kind:       "stream",
		Region:     "local",
		PeerRegion: "peer",
		OwnerID:    "pod-1",
		Generation: 2,
	}
	secondLease := models.Lease{
		Kind:       "stream",
		Region:     "local",
		PeerRegion: "peer",
		OwnerID:    "pod-1",
		Generation: 3,
	}
	store.EXPECT().
		TryStateAcquireLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", time.Second).
		Return(firstLease, true, nil)
	store.EXPECT().
		ReleaseStateLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", int64(2)).
		Return(true, nil)
	store.EXPECT().
		TryStateAcquireLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", time.Second).
		Return(secondLease, true, nil)
	store.EXPECT().
		ReleaseStateLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", int64(3)).
		Return(true, nil)

	calls := 0
	err := service.RunWithLease(ctx, func(context.Context) error {
		calls++
		if calls == 1 {
			return wantErr
		}
		cancel()
		return nil
	})
	if err != nil {
		t.Fatalf("RunWithLease() error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("fn calls = %d, want 2", calls)
	}
}

func TestRunWithLease_CancelsSessionWhenRenewFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	service := NewService(store, Config{
		Kind:          "stream",
		Region:        "local",
		PeerRegion:    "peer",
		OwnerID:       "pod-1",
		TTL:           time.Second,
		RenewInterval: 5 * time.Millisecond,
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lease := models.Lease{
		Kind:       "stream",
		Region:     "local",
		PeerRegion: "peer",
		OwnerID:    "pod-1",
		Generation: 7,
	}
	store.EXPECT().
		TryStateAcquireLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", time.Second).
		Return(lease, true, nil)
	store.EXPECT().
		RenewStateLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", int64(7), time.Second).
		Return(models.Lease{}, false, nil)
	store.EXPECT().
		ReleaseStateLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", int64(7)).
		Return(true, nil)

	err := service.RunWithLease(ctx, func(sessionCtx context.Context) error {
		<-sessionCtx.Done()
		cancel()
		return errors.New("lease lost")
	})
	if err != nil {
		t.Fatalf("RunWithLease() error = %v", err)
	}
}

func TestRunWithLease_ReturnsPermanentFunctionError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	service := NewService(store, Config{
		Kind:          "stream",
		Region:        "local",
		PeerRegion:    "peer",
		OwnerID:       "pod-1",
		TTL:           time.Second,
		RenewInterval: 5 * time.Millisecond,
	}, nil)

	lease := models.Lease{
		Kind:       "stream",
		Region:     "local",
		PeerRegion: "peer",
		OwnerID:    "pod-1",
		Generation: 11,
	}
	store.EXPECT().
		TryStateAcquireLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", time.Second).
		Return(lease, true, nil)
	store.EXPECT().
		ReleaseStateLease(gomock.Any(), "stream", "local", "peer", "", "pod-1", int64(11)).
		Return(true, nil)

	wantErr := errors.Join(models.ErrChangefeedCursorSourceMismatch, errors.New("fatal"))
	err := service.RunWithLease(context.Background(), func(context.Context) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("RunWithLease() error = %v, want %v", err, wantErr)
	}
}
