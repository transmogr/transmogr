package snapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/transmogr/transmogr/internal/models"
)

func TestStateServiceSendToPeerUsesSnapshotLease(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	state := NewMocksnapshotState(ctrl)
	source := NewMockSource(ctrl)
	applier := NewMockApplier(ctrl)

	service := NewServiceWithPipelineAndLeases(
		state,
		source,
		applier,
		100,
		&LeaseConfig{
			LocalRegion:   "local",
			InstanceID:    "pod-a",
			TTL:           time.Second,
			RenewInterval: time.Hour,
		},
	)

	lease := models.Lease{
		Kind:       "snapshot",
		Region:     "local",
		PeerRegion: "eu",
		Table:      "users",
		OwnerID:    "pod-a",
		Generation: 1,
	}
	gomock.InOrder(
		state.EXPECT().
			TryStateAcquireLease(gomock.Any(), "snapshot", "local", "eu", "users", "pod-a", time.Second).
			Return(lease, true, nil),
		source.EXPECT().ReadSnapshotChunk(gomock.Any(), models.SnapshotReadRequest{
			LocalRegion: "local",
			Table:       "users",
			Limit:       100,
		}).Return(models.SnapshotChunk{
			Rows: []models.SnapshotRow{{PrimaryKey: models.PrimaryKeyString("id", "pk-1")}},
		}, nil),
		source.EXPECT().ReadSnapshotChunk(gomock.Any(), models.SnapshotReadRequest{
			LocalRegion:     "local",
			Table:           "users",
			AfterPrimaryKey: models.PrimaryKeyString("id", "pk-1"),
			Limit:           100,
		}).Return(models.SnapshotChunk{
			Rows: []models.SnapshotRow{{PrimaryKey: models.PrimaryKeyString("id", "pk-2")}},
			Last: true,
		}, nil),
		state.EXPECT().
			ReleaseStateLease(gomock.Any(), "snapshot", "local", "eu", "users", "pod-a", int64(1)).
			Return(true, nil),
	)

	var sent []models.SnapshotChunk
	if err := service.SendToPeer(context.Background(), "eu", "users", nil, func(chunk models.SnapshotChunk) error {
		sent = append(sent, chunk)
		return nil
	}); err != nil {
		t.Fatalf("send snapshot: %v", err)
	}

	if len(sent) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(sent))
	}
}

func TestStateServiceSendToPeerReturnsLeaseErrorWhenLeaseNotOwned(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	state := NewMocksnapshotState(ctrl)
	service := NewServiceWithPipelineAndLeases(
		state,
		NewMockSource(ctrl),
		NewMockApplier(ctrl),
		100,
		&LeaseConfig{
			LocalRegion:   "local",
			InstanceID:    "pod-a",
			TTL:           time.Second,
			RenewInterval: time.Hour,
		},
	)

	state.EXPECT().
		TryStateAcquireLease(gomock.Any(), "snapshot", "local", "eu", "users", "pod-a", time.Second).
		Return(models.Lease{}, false, nil)

	called := false
	if err := service.SendToPeer(context.Background(), "eu", "users", nil, func(models.SnapshotChunk) error {
		called = true
		return nil
	}); !errors.Is(err, models.ErrLeaseNotAcquired) {
		t.Fatalf("send snapshot without lease: %v", err)
	}
	if called {
		t.Fatal("expected send callback to be skipped without lease ownership")
	}
}

func TestStateServiceSendToPeerResumesAfterPrimaryKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	state := NewMocksnapshotState(ctrl)
	source := NewMockSource(ctrl)
	applier := NewMockApplier(ctrl)

	service := NewServiceWithPipelineAndLeases(state, source, applier, 100, nil)

	source.EXPECT().ReadSnapshotChunk(gomock.Any(), models.SnapshotReadRequest{
		Table:           "users",
		AfterPrimaryKey: models.PrimaryKeyString("id", "pk-7"),
		Limit:           100,
	}).Return(models.SnapshotChunk{
		Rows: []models.SnapshotRow{{PrimaryKey: models.PrimaryKeyString("id", "pk-8")}},
		Last: true,
	}, nil)

	var sent []models.SnapshotChunk
	if err := service.SendToPeer(
		context.Background(),
		"eu",
		"users",
		models.PrimaryKeyString("id", "pk-7"),
		func(chunk models.SnapshotChunk) error {
			sent = append(sent, chunk)
			return nil
		},
	); err != nil {
		t.Fatalf("send resumed snapshot: %v", err)
	}

	if len(sent) != 1 {
		t.Fatalf("expected 1 resumed chunk, got %d", len(sent))
	}
}

func TestStateServiceApplyIncomingChunkPreservesCheckpointOnEmptyLastChunk(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	state := NewMocksnapshotState(ctrl)
	applier := NewMockApplier(ctrl)

	service := NewServiceWithPipelineAndLeases(state, NewMockSource(ctrl), applier, 100, nil)

	existingPK := models.PrimaryKeyString("id", "pk-42")
	watermark := time.Unix(123, 0).UTC()

	gomock.InOrder(
		state.EXPECT().
			GetStateSnapshotJob(gomock.Any(), "eu", "users").
			Return(models.StateSnapshotJob{
				PeerRegion:     "eu",
				Table:          "users",
				Status:         models.SnapshotJobStatusSnapshotting,
				LastPrimaryKey: existingPK,
			}, nil),
		state.EXPECT().
			UpsertStateSnapshotJob(gomock.Any(), models.StateSnapshotJob{
				PeerRegion:     "eu",
				Table:          "users",
				Status:         models.SnapshotJobStatusCatchingUp,
				LastPrimaryKey: existingPK,
				WatermarkAt:    watermark,
			}).
			Return(nil),
	)

	err := service.ApplyIncomingChunk(context.Background(), "eu", "users", models.SnapshotChunk{
		Last:      true,
		Watermark: watermark,
	})
	if err != nil {
		t.Fatalf("apply empty last chunk: %v", err)
	}
}
