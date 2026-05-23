package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/transmogr/transmogr/internal/models"
)

func TestNewWithRepositoryRequiresRepository(t *testing.T) {
	t.Parallel()

	svc, err := NewWithRepository(nil)
	require.Nil(t, svc)
	require.EqualError(t, err, "outbox repository is required")
}

func TestNextSequenceDelegatesToRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := NewMockrepository(ctrl)
	svc, err := NewWithRepository(repo)
	require.NoError(t, err)

	ctx := context.Background()
	repo.EXPECT().OutboxNextSequence(ctx, "eu", "users").Return(uint64(42), nil)

	seq, err := svc.NextSequence(ctx, "eu", "users")
	require.NoError(t, err)
	require.Equal(t, uint64(42), seq)
}

func TestEnqueuePublishesInsertedBatchToSubscribers(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := NewMockrepository(ctrl)
	svc, err := NewWithRepository(repo)
	require.NoError(t, err)

	ctx := context.Background()
	ch, unsubscribe, err := svc.Subscribe(ctx, "eu")
	require.NoError(t, err)
	defer unsubscribe()

	input := models.OutboxBatch{PeerRegion: "eu", Sequence: 1}
	stored := models.OutboxBatch{PeerRegion: "eu", Sequence: 2}
	repo.EXPECT().OutboxEnqueue(ctx, input).Return(stored, true, nil)

	err = svc.Enqueue(ctx, input)
	require.NoError(t, err)

	select {
	case got := <-ch:
		require.Equal(t, stored, got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published batch")
	}
}

func TestEnqueueDoesNotPublishWhenRepositoryReportsExistingBatch(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := NewMockrepository(ctrl)
	svc, err := NewWithRepository(repo)
	require.NoError(t, err)

	ctx := context.Background()
	ch, unsubscribe, err := svc.Subscribe(ctx, "eu")
	require.NoError(t, err)
	defer unsubscribe()

	batch := models.OutboxBatch{PeerRegion: "eu", Sequence: 1}
	repo.EXPECT().OutboxEnqueue(ctx, batch).Return(models.OutboxBatch{}, false, nil)

	err = svc.Enqueue(ctx, batch)
	require.NoError(t, err)

	select {
	case got := <-ch:
		t.Fatalf("unexpected published batch: %+v", got)
	default:
	}
}

func TestSubscribeUnsubscribeClosesChannelAndRemovesSubscriber(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := NewMockrepository(ctrl)
	svc, err := NewWithRepository(repo)
	require.NoError(t, err)

	ch, unsubscribe, err := svc.Subscribe(context.Background(), "eu")
	require.NoError(t, err)

	unsubscribe()
	unsubscribe()

	_, ok := <-ch
	require.False(t, ok)
	require.Empty(t, svc.subs)
}

func TestListPendingWindowDelegatesToRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := NewMockrepository(ctrl)
	svc, err := NewWithRepository(repo)
	require.NoError(t, err)

	ctx := context.Background()
	lower := time.Unix(10, 0)
	upper := time.Unix(20, 0)
	expected := []models.OutboxBatch{{PeerRegion: "eu", Sequence: 7}}
	repo.EXPECT().
		OutboxListPendingWindow(ctx, "eu", lower, upper, uint64(3), 25).
		Return(expected, nil)

	got, err := svc.ListPendingWindow(ctx, "eu", lower, upper, 3, 25)
	require.NoError(t, err)
	require.Equal(t, expected, got)
}

func TestAckSnapshotWatermarkAndReadyDelegateToRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := NewMockrepository(ctrl)
	svc, err := NewWithRepository(repo)
	require.NoError(t, err)

	ctx := context.Background()
	watermark := time.Unix(123, 0)

	repo.EXPECT().OutboxAck(ctx, "eu", "users", "evt-1").Return(true, nil)
	repo.EXPECT().OutboxSnapshotWatermark(ctx).Return(watermark, nil)
	repo.EXPECT().OutboxReady(ctx).Return(nil)

	acked, err := svc.Ack(ctx, "eu", "users", "evt-1")
	require.NoError(t, err)
	require.True(t, acked)

	gotWatermark, err := svc.SnapshotWatermark(ctx)
	require.NoError(t, err)
	require.Equal(t, watermark, gotWatermark)

	require.NoError(t, svc.Ready(ctx))
}

func TestCleanupUpdatesMetricsCounters(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := NewMockrepository(ctrl)
	svc, err := NewWithRepository(repo)
	require.NoError(t, err)

	ctx := context.Background()
	repo.EXPECT().OutboxCleanup(ctx, 2*time.Hour, 10).Return(int64(2), int64(3), nil)

	deleted, err := svc.Cleanup(ctx, 2*time.Hour, 10)
	require.NoError(t, err)
	require.Equal(t, int64(5), deleted)

	snapshot := svc.OutboxMetricsSnapshot()
	require.EqualValues(t, 5, snapshot.CleanupDeleted)
	require.EqualValues(t, 2, snapshot.CleanupDeletedByAge)
	require.EqualValues(t, 3, snapshot.CleanupDeletedByCount)
}

func TestRunCleanupRequiresPositiveInterval(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := NewMockrepository(ctrl)
	svc, err := NewWithRepository(repo)
	require.NoError(t, err)

	err = svc.RunCleanup(context.Background(), 0, time.Hour, 10)
	require.EqualError(t, err, "outbox cleanup interval must be positive")
}

func TestRunCleanupIgnoresRepositoryErrorsUntilContextCanceled(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := NewMockrepository(ctrl)
	svc, err := NewWithRepository(repo)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo.EXPECT().
		OutboxCleanup(gomock.Any(), time.Hour, 10).
		DoAndReturn(func(context.Context, time.Duration, int) (int64, int64, error) {
			cancel()
			return 0, 0, errors.New("boom")
		})

	err = svc.RunCleanup(ctx, time.Millisecond, time.Hour, 10)
	require.NoError(t, err)
}

func TestRunMetricsRefreshLoadsSnapshotAndExitsOnContextCancel(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := NewMockrepository(ctrl)
	svc, err := NewWithRepository(repo)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expected := models.OutboxMetricsSnapshot{
		PendingBatches: 4,
		OldestBatchAge: 3 * time.Minute,
	}
	repo.EXPECT().
		OutboxLoadMetricsSnapshot(gomock.Any()).
		DoAndReturn(func(context.Context) (models.OutboxMetricsSnapshot, error) {
			cancel()
			return expected, nil
		})

	err = svc.RunMetricsRefresh(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, expected.PendingBatches, svc.OutboxMetricsSnapshot().PendingBatches)
	require.Equal(t, expected.OldestBatchAge, svc.OutboxMetricsSnapshot().OldestBatchAge)
}
