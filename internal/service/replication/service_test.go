package replication

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/transmogr/transmogr/internal/models"
)

func TestApplyEventBatchSortsPollingEventsByCursorPosition(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	state := NewMockreplicationState(ctrl)
	svc := NewService(
		"local",
		nil,
		"test",
		"cfg",
		models.ReplicationApplyModeFullSync,
		nil,
		state,
		nil,
		nil,
	)

	events := []models.ReplicationEvent{
		{
			PeerRegion: "eu",
			Table:      "users",
			EventID:    "evt-2",
			PrimaryKey: models.PrimaryKeyString("id", "user-2"),
			Operation:  models.ReplicationOperationUpsert,
			Version:    42,
		},
		{
			PeerRegion: "eu",
			Table:      "users",
			EventID:    "evt-1",
			PrimaryKey: models.PrimaryKeyString("id", "user-1"),
			Operation:  models.ReplicationOperationUpsert,
			Version:    42,
		},
	}

	state.EXPECT().
		CommitStateAppliedTransaction(gomock.Any(), gomock.Any(), models.ReplicationApplyModeFullSync).
		DoAndReturn(func(_ context.Context, got []models.ReplicationEvent, _ models.ReplicationApplyMode) error {
			if len(got) != 2 {
				t.Fatalf("unexpected event count: %d", len(got))
			}
			if got[0].EventID != "evt-1" || got[1].EventID != "evt-2" {
				t.Fatalf("unexpected event order: %#v", got)
			}
			return nil
		})

	if err := svc.ApplyEventBatch(context.Background(), events); err != nil {
		t.Fatalf("apply event batch: %v", err)
	}
}

func TestApplyTransactionBatchPreservesCDCOrder(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	state := NewMockreplicationState(ctrl)
	svc := NewService(
		"local",
		nil,
		"test",
		"cfg",
		models.ReplicationApplyModeFullSync,
		nil,
		state,
		nil,
		nil,
	)

	events := []models.ReplicationEvent{
		{
			PeerRegion: "eu",
			Table:      "users",
			EventID:    "evt-2",
			PrimaryKey: models.PrimaryKeyString("id", "user-2"),
			Operation:  models.ReplicationOperationUpdate,
			Version:    42,
		},
		{
			PeerRegion: "eu",
			Table:      "users",
			EventID:    "evt-1",
			PrimaryKey: models.PrimaryKeyString("id", "user-1"),
			Operation:  models.ReplicationOperationDelete,
			Version:    42,
		},
	}

	state.EXPECT().
		CommitStateAppliedTransaction(gomock.Any(), gomock.Any(), models.ReplicationApplyModeFullSync).
		DoAndReturn(func(_ context.Context, got []models.ReplicationEvent, _ models.ReplicationApplyMode) error {
			if len(got) != 2 {
				t.Fatalf("unexpected event count: %d", len(got))
			}
			if got[0].EventID != "evt-2" || got[1].EventID != "evt-1" {
				t.Fatalf("unexpected event order: %#v", got)
			}
			return nil
		})

	if err := svc.ApplyTransactionBatch(context.Background(), events); err != nil {
		t.Fatalf("apply transaction batch: %v", err)
	}
}
