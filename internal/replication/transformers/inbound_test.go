package transformers

import (
	"context"
	"errors"
	"testing"

	"github.com/transmogr/transmogr/internal/models"
)

func TestInboundPipelineClonesAndPreparesEvents(t *testing.T) {
	t.Parallel()

	pipeline := NewInbound(
		inboundTransformerFunc(func(
			_ context.Context,
			events []models.ReplicationEvent,
		) ([]models.ReplicationEvent, error) {
			delete(events[0].Metadata, "virtual.full_name")
			events[0].Fields["name"] = []byte(`"ciphertext"`)
			return events, nil
		}),
	)

	original := []models.ReplicationEvent{
		{
			EventID: "evt-1",
			Fields: map[string][]byte{
				"name": []byte(`"alice"`),
			},
			Metadata: map[string][]byte{
				"virtual.full_name": []byte(`"Alice Smith"`),
			},
		},
	}

	prepared, err := pipeline.PrepareEvents(context.Background(), original)
	if err != nil {
		t.Fatalf("prepare events: %v", err)
	}

	if got := string(prepared[0].Fields["name"]); got != `"ciphertext"` {
		t.Fatalf("unexpected prepared field: %s", got)
	}
	if _, ok := prepared[0].Metadata["virtual.full_name"]; ok {
		t.Fatal("expected metadata to be removed by inbound pipeline")
	}
	if got := string(original[0].Fields["name"]); got != `"alice"` {
		t.Fatalf("original events mutated: %s", got)
	}
	if _, ok := original[0].Metadata["virtual.full_name"]; !ok {
		t.Fatal("original metadata mutated")
	}
}

func TestInboundMetadataTransformerRemovesTransitMetadataAndValidatesKeys(t *testing.T) {
	t.Parallel()

	prepared, err := NewInbound(NewInboundMetadataTransformer()).PrepareEvents(
		context.Background(),
		[]models.ReplicationEvent{
			{
				EventID: "evt-1",
				Metadata: map[string][]byte{
					"trace.request_id":         []byte(`"req-1"`),
					"transit.virtual.fullname": []byte(`"Alice"`),
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("prepare metadata: %v", err)
	}

	if _, ok := prepared[0].Metadata["transit.virtual.fullname"]; ok {
		t.Fatal("expected transit metadata to be removed")
	}
	if got := string(prepared[0].Metadata["trace.request_id"]); got != `"req-1"` {
		t.Fatalf("expected regular metadata to remain, got %s", got)
	}

	_, err = NewInbound(NewInboundMetadataTransformer()).PrepareEvents(
		context.Background(),
		[]models.ReplicationEvent{
			{
				EventID: "evt-2",
				Metadata: map[string][]byte{
					"Trace.RequestID": []byte(`"bad"`),
				},
			},
		},
	)
	if err == nil {
		t.Fatal("expected invalid metadata key to fail validation")
	}

	preparedRows, err := NewInbound(
		NewInboundMetadataTransformer(),
	).PrepareSnapshotRows(
		context.Background(),
		"users",
		[]models.SnapshotRow{
			{
				Metadata: map[string][]byte{
					"trace.request_id":          []byte(`"req-1"`),
					"transit.virtual.full_name": []byte(`"Alice"`),
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("prepare snapshot metadata: %v", err)
	}
	if _, ok := preparedRows[0].Metadata["transit.virtual.full_name"]; ok {
		t.Fatal("expected transit snapshot metadata to be removed")
	}
}

func TestCryptoInboundTransformerSkipsDeleteEvents(t *testing.T) {
	t.Parallel()

	prepared, err := NewCryptoInboundTransformer(fakeCryptoService{
		encryptFn: func(context.Context, string, map[string][]byte) (map[string][]byte, error) {
			return nil, errors.New("encrypt should not be called for delete")
		},
	}).PrepareInboundEvents(
		context.Background(),
		[]models.ReplicationEvent{
			{
				EventID:   "evt-1",
				Table:     "users",
				Operation: models.ReplicationOperationDelete,
				Metadata: map[string][]byte{
					TransitKey("secret"): []byte(`"plain"`),
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("prepare delete event: %v", err)
	}
	if _, ok := prepared[0].Metadata[TransitKey("secret")]; !ok {
		t.Fatal("expected delete event metadata to remain untouched")
	}
}

func TestCryptoInboundTransformerReturnsEncryptError(t *testing.T) {
	t.Parallel()

	_, err := NewCryptoInboundTransformer(fakeCryptoService{
		encryptFn: func(context.Context, string, map[string][]byte) (map[string][]byte, error) {
			return nil, errors.New("encrypt failed")
		},
	}).PrepareInboundEvents(
		context.Background(),
		[]models.ReplicationEvent{
			{
				EventID: "evt-1",
				Table:   "users",
				Metadata: map[string][]byte{
					TransitKey("secret"): []byte(`"plain"`),
				},
			},
		},
	)
	if err == nil {
		t.Fatal("expected encrypt error")
	}
	if got := err.Error(); got != `prepare inbound event "evt-1": encrypt failed` {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestCryptoInboundTransformerSnapshotReturnsDecodeError(t *testing.T) {
	t.Parallel()

	_, err := NewCryptoInboundTransformer(fakeCryptoService{}).PrepareInboundSnapshotRows(
		context.Background(),
		"users",
		[]models.SnapshotRow{{Payload: []byte("not-json")}},
	)
	if err == nil {
		t.Fatal("expected snapshot decode error")
	}
}

type inboundTransformerFunc func(context.Context, []models.ReplicationEvent) ([]models.ReplicationEvent, error)

func (f inboundTransformerFunc) PrepareInboundEvents(
	ctx context.Context,
	events []models.ReplicationEvent,
) ([]models.ReplicationEvent, error) {
	return f(ctx, events)
}

func (f inboundTransformerFunc) PrepareInboundSnapshotRows(
	_ context.Context,
	_ string,
	rows []models.SnapshotRow,
) ([]models.SnapshotRow, error) {
	return rows, nil
}
