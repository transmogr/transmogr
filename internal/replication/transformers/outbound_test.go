package transformers

import (
	"context"
	"errors"
	"testing"

	"github.com/transmogr/transmogr/internal/models"
)

func TestOutboundPipelineClonesAndPreparesBatch(t *testing.T) {
	t.Parallel()

	pipeline := NewOutbound(
		outboundTransformerFunc(func(
			_ context.Context,
			batch models.OutboxBatch,
		) (models.OutboxBatch, error) {
			batch.Events[0].Fields["name"] = []byte(`"alice"`)
			batch.Events[0].Metadata["virtual.full_name"] = []byte(`"Alice Smith"`)
			return batch, nil
		}),
	)

	original := models.OutboxBatch{
		PeerRegion: "remote",
		Events: []models.ReplicationEvent{
			{
				EventID: "evt-1",
				Fields: map[string][]byte{
					"name": []byte(`"ciphertext"`),
				},
				Metadata: map[string][]byte{},
			},
		},
	}

	prepared, err := pipeline.PrepareBatch(context.Background(), original)
	if err != nil {
		t.Fatalf("prepare batch: %v", err)
	}

	if got := string(prepared.Events[0].Fields["name"]); got != `"alice"` {
		t.Fatalf("unexpected prepared field: %s", got)
	}
	if got := string(prepared.Events[0].Metadata["virtual.full_name"]); got != `"Alice Smith"` {
		t.Fatalf("unexpected prepared metadata: %s", got)
	}
	if got := string(original.Events[0].Fields["name"]); got != `"ciphertext"` {
		t.Fatalf("original batch mutated: %s", got)
	}
	if _, ok := original.Events[0].Metadata["virtual.full_name"]; ok {
		t.Fatal("original metadata mutated")
	}
}

func TestCryptoTransformersMoveEncryptedFieldsThroughMetadata(t *testing.T) {
	t.Parallel()

	crypto := fakeCryptoService{
		decryptFn: func(_ context.Context, _ string, fields map[string][]byte) (map[string][]byte, error) {
			projected := models.CloneFields(fields)
			projected["secret"] = []byte(`"plain"`)
			return projected, nil
		},
		encryptFn: func(_ context.Context, _ string, fields map[string][]byte) (map[string][]byte, error) {
			projected := models.CloneFields(fields)
			projected["secret"] = []byte(`"cipher"`)
			return projected, nil
		},
		fieldsByTable: map[string][]string{"users": {"secret"}},
	}

	outboundBatch, err := NewOutbound(
		NewCryptoOutboundTransformer(crypto),
	).PrepareBatch(
		context.Background(),
		models.OutboxBatch{
			Events: []models.ReplicationEvent{
				{
					Table: "users",
					Fields: map[string][]byte{
						"secret": []byte(`"stored-cipher"`),
						"name":   []byte(`"alice"`),
					},
					Metadata: map[string][]byte{
						"trace.request_id": []byte(`"req-1"`),
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("prepare outbound crypto: %v", err)
	}

	if _, ok := outboundBatch.Events[0].Fields["secret"]; ok {
		t.Fatal("expected encrypted field to be removed from outbound fields")
	}
	if got := string(
		outboundBatch.Events[0].Metadata[TransitKey("secret")],
	); got != `"plain"` {
		t.Fatalf("unexpected outbound crypto metadata: %s", got)
	}

	inboundEvents, err := NewInbound(
		NewCryptoInboundTransformer(crypto),
	).PrepareEvents(context.Background(), outboundBatch.Events)
	if err != nil {
		t.Fatalf("prepare inbound crypto: %v", err)
	}

	if got := string(inboundEvents[0].Fields["secret"]); got != `"cipher"` {
		t.Fatalf("unexpected inbound encrypted field: %s", got)
	}
	if _, ok := inboundEvents[0].Metadata[TransitKey("secret")]; ok {
		t.Fatal("expected crypto metadata to be removed after inbound restore")
	}
	if got := string(inboundEvents[0].Metadata["trace.request_id"]); got != `"req-1"` {
		t.Fatalf("expected non-crypto metadata to remain, got %s", got)
	}
}

func TestCryptoTransformersMoveEncryptedSnapshotFieldsThroughMetadata(t *testing.T) {
	t.Parallel()

	crypto := fakeCryptoService{
		decryptFn: func(_ context.Context, _ string, fields map[string][]byte) (map[string][]byte, error) {
			projected := models.CloneFields(fields)
			projected["secret"] = []byte(`"plain"`)
			return projected, nil
		},
		encryptFn: func(_ context.Context, _ string, fields map[string][]byte) (map[string][]byte, error) {
			projected := models.CloneFields(fields)
			projected["secret"] = []byte(`"cipher"`)
			return projected, nil
		},
		fieldsByTable: map[string][]string{"users": {"secret"}},
	}

	payload, err := models.FieldsToPayload(map[string][]byte{
		"secret": []byte(`"stored-cipher"`),
		"name":   []byte(`"alice"`),
	})
	if err != nil {
		t.Fatalf("encode snapshot payload: %v", err)
	}

	rows, err := NewCryptoOutboundTransformer(crypto).PrepareOutboundSnapshotRows(
		context.Background(),
		"users",
		[]models.SnapshotRow{
			{
				Payload: payload,
				Metadata: map[string][]byte{
					"trace.request_id": []byte(`"req-1"`),
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("prepare outbound snapshot: %v", err)
	}

	outboundFields, err := models.PayloadToFields(rows[0].Payload)
	if err != nil {
		t.Fatalf("decode outbound snapshot payload: %v", err)
	}
	if _, ok := outboundFields["secret"]; ok {
		t.Fatal("expected secret to be removed from outbound snapshot payload")
	}
	if got := string(rows[0].Metadata[TransitKey("secret")]); got != `"plain"` {
		t.Fatalf("unexpected outbound snapshot crypto metadata: %s", got)
	}

	rows, err = NewCryptoInboundTransformer(crypto).PrepareInboundSnapshotRows(context.Background(), "users", rows)
	if err != nil {
		t.Fatalf("prepare inbound snapshot: %v", err)
	}

	restoredFields, err := models.PayloadToFields(rows[0].Payload)
	if err != nil {
		t.Fatalf("decode restored snapshot payload: %v", err)
	}
	if got := string(restoredFields["secret"]); got != `"cipher"` {
		t.Fatalf("unexpected restored snapshot field: %s", got)
	}
	if _, ok := rows[0].Metadata[TransitKey("secret")]; ok {
		t.Fatal("expected snapshot crypto metadata to be consumed")
	}
	if got := string(rows[0].Metadata["trace.request_id"]); got != `"req-1"` {
		t.Fatalf("expected regular snapshot metadata to remain, got %s", got)
	}
}

func TestCryptoTransitKeyHelpers(t *testing.T) {
	t.Parallel()

	key := TransitKey("secret")
	if key != "transit.crypto.secret" {
		t.Fatalf("unexpected crypto transit key: %s", key)
	}

	field, ok := ParseTransitKey(key)
	if !ok || field != "secret" {
		t.Fatalf("unexpected parsed crypto transit key: field=%q ok=%v", field, ok)
	}

	if _, ok := ParseTransitKey("trace.request_id"); ok {
		t.Fatal("expected non-crypto key to be rejected")
	}
	if _, ok := ParseTransitKey("transit.crypto.Secret"); ok {
		t.Fatal("expected invalid crypto field key to be rejected")
	}
}

func TestCryptoOutboundTransformerReturnsDecryptError(t *testing.T) {
	t.Parallel()

	transformer := NewCryptoOutboundTransformer(fakeCryptoService{
		decryptFn: func(context.Context, string, map[string][]byte) (map[string][]byte, error) {
			return nil, errors.New("decrypt failed")
		},
	})

	_, err := transformer.PrepareOutboundBatch(context.Background(), models.OutboxBatch{
		Events: []models.ReplicationEvent{
			{EventID: "evt-1", Table: "users"},
		},
	})
	if err == nil {
		t.Fatal("expected decrypt error")
	}
	if got := err.Error(); got != `prepare outbound batch event "evt-1": decrypt failed` {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestCryptoOutboundTransformerSnapshotReturnsDecodeError(t *testing.T) {
	t.Parallel()

	_, err := NewCryptoOutboundTransformer(fakeCryptoService{}).PrepareOutboundSnapshotRows(
		context.Background(),
		"users",
		[]models.SnapshotRow{{Payload: []byte("not-json")}},
	)
	if err == nil {
		t.Fatal("expected snapshot decode error")
	}
}

func TestCryptoOutboundTransformerWithoutCryptoIsNoop(t *testing.T) {
	t.Parallel()

	original := models.OutboxBatch{
		Events: []models.ReplicationEvent{
			{
				Table: "users",
				Fields: map[string][]byte{
					"name": []byte(`"alice"`),
				},
			},
		},
	}

	prepared, err := NewCryptoOutboundTransformer(nil).PrepareOutboundBatch(context.Background(), original)
	if err != nil {
		t.Fatalf("prepare outbound noop: %v", err)
	}
	if got := string(prepared.Events[0].Fields["name"]); got != `"alice"` {
		t.Fatalf("unexpected noop field: %s", got)
	}
}

type outboundTransformerFunc func(context.Context, models.OutboxBatch) (models.OutboxBatch, error)

func (f outboundTransformerFunc) PrepareOutboundBatch(
	ctx context.Context,
	batch models.OutboxBatch,
) (models.OutboxBatch, error) {
	return f(ctx, batch)
}

func (f outboundTransformerFunc) PrepareOutboundSnapshotRows(
	_ context.Context,
	_ string,
	rows []models.SnapshotRow,
) ([]models.SnapshotRow, error) {
	return rows, nil
}

type fakeCryptoService struct {
	encryptFn     func(context.Context, string, map[string][]byte) (map[string][]byte, error)
	decryptFn     func(context.Context, string, map[string][]byte) (map[string][]byte, error)
	fieldsByTable map[string][]string
}

func (f fakeCryptoService) Encrypt(
	ctx context.Context,
	table string,
	fields map[string][]byte,
) (map[string][]byte, error) {
	if f.encryptFn == nil {
		return fields, nil
	}
	return f.encryptFn(ctx, table, fields)
}

func (f fakeCryptoService) Decrypt(
	ctx context.Context,
	table string,
	fields map[string][]byte,
) (map[string][]byte, error) {
	if f.decryptFn == nil {
		return fields, nil
	}
	return f.decryptFn(ctx, table, fields)
}

func (f fakeCryptoService) EncryptedFieldNames(table string) []string {
	return append([]string(nil), f.fieldsByTable[table]...)
}
