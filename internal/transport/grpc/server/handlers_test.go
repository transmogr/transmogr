package server

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/transmogr/transmogr/internal/models"
	transformers "github.com/transmogr/transmogr/internal/replication/transformers"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

func TestConsumeSendsHandshakeAndForwardsOutbound(t *testing.T) {
	t.Parallel()

	cfg := models.BuildHandshakeConfiguration("cdc", nil, nil)
	outbound := make(chan models.OutboxBatch, 1)
	outbound <- models.OutboxBatch{
		PeerRegion: "remote",
		Table:      "users",
		Sequence:   1,
		Events: []models.ReplicationEvent{
			{
				PeerRegion: "local",
				Table:      "users",
				EventID:    "evt-1",
				PrimaryKey: []models.PrimaryKeyPart{{Column: "id", Type: models.PrimaryKeyTypeString, Value: []byte("1")}},
				Fields: map[string][]byte{
					"name": []byte(`"ciphertext"`),
				},
			},
		},
	}
	close(outbound)

	service := &fakeServerReplicationService{
		handshake: models.HandshakeMessage{
			Region:        "local",
			Version:       "v1",
			Configuration: cfg,
		},
	}
	outbox := &fakeAckOutbox{live: outbound}
	handler := &Handler{
		service: service,
		outbox:  outbox,
		pipeline: transformers.NewOutbound(
			transformers.NewCryptoOutboundTransformer(fakeCryptoService{
				decryptFn: func(
					_ context.Context,
					_ string,
					fields map[string][]byte,
				) (map[string][]byte, error) {
					projected := models.CloneFields(fields)
					projected["name"] = []byte(`"alice"`)
					return projected, nil
				},
				fieldsByTable: map[string][]string{"users": {"name"}},
			}),
		),
		localRegion:        "local",
		allowedPeerRegions: map[string]struct{}{"remote": {}},
	}
	stream := &fakeConsumeServer{ctx: context.Background()}

	err := handler.Consume(&peerv1.ConsumeRequest{
		Region:        "remote",
		Version:       "v1",
		Configuration: cfg,
	}, stream)
	if err != nil {
		t.Fatalf("Consume returned error: %v", err)
	}
	if len(stream.sent) != 2 {
		t.Fatalf("expected handshake and one event batch, got %d messages", len(stream.sent))
	}
	if stream.sent[0].GetHandshake().GetRegion() != "local" {
		t.Fatalf("unexpected handshake: %#v", stream.sent[0])
	}
	if len(stream.sent[1].GetEventBatch().GetEvents()) != 1 {
		t.Fatalf("unexpected forwarded batch: %#v", stream.sent[1])
	}
	if _, ok := stream.sent[1].GetEventBatch().GetEvents()[0].GetFields()["name"]; ok {
		t.Fatal("expected encrypted field to be removed from wire fields")
	}
	if got := string(
		stream.sent[1].GetEventBatch().GetEvents()[0].GetMetadata()[transformers.TransitKey("name")],
	); got != `"alice"` {
		t.Fatalf("expected outbound pipeline to move decrypted field into metadata, got %s", got)
	}
}

func TestAckValidatesPeerRegion(t *testing.T) {
	t.Parallel()

	outbox := &fakeAckOutbox{live: make(chan models.OutboxBatch)}
	handler := &Handler{
		service:            &fakeServerReplicationService{},
		outbox:             outbox,
		localRegion:        "local",
		allowedPeerRegions: map[string]struct{}{"remote": {}},
	}

	if _, err := handler.Ack(
		context.Background(),
		&peerv1.AckRequest{Region: "remote", Table: "users", LastEventId: "evt-1"},
	); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	if outbox.peerRegion != "remote" || outbox.table != "users" || outbox.lastEventID != "evt-1" {
		t.Fatalf("unexpected outbox ack: %#v", outbox)
	}
	if _, err := handler.Ack(context.Background(), &peerv1.AckRequest{Region: "denied"}); err == nil {
		t.Fatal("expected Ack to reject disallowed region")
	}
}

func TestConsumeProjectsSnapshotChunk(t *testing.T) {
	t.Parallel()

	cfg := models.BuildHandshakeConfiguration("cdc", nil, nil)
	service := &fakeServerReplicationService{
		handshake: models.HandshakeMessage{Region: "local", Version: "v1", Configuration: cfg},
		sendSnapshotFn: func(
			_ context.Context,
			_ string,
			_ string,
			_ []models.PrimaryKeyPart,
			send func(models.SnapshotChunkMessage) error,
		) error {
			return send(models.SnapshotChunkMessage{
				Table: "users",
				Rows: []models.SnapshotRow{
					{
						PrimaryKey: []models.PrimaryKeyPart{{Column: "id", Type: models.PrimaryKeyTypeString, Value: []byte(`"1"`)}},
						Payload: mustPayload(t, map[string][]byte{
							"name": []byte(`"ciphertext"`),
						}),
					},
				},
				Last: true,
			})
		},
	}
	handler := &Handler{
		service: service,
		outbox:  &fakeAckOutbox{live: closedOutbox()},
		pipeline: transformers.NewOutbound(
			transformers.NewCryptoOutboundTransformer(fakeCryptoService{
				decryptFn: func(
					_ context.Context,
					_ string,
					fields map[string][]byte,
				) (map[string][]byte, error) {
					projected := models.CloneFields(fields)
					projected["name"] = []byte(`"alice"`)
					return projected, nil
				},
				fieldsByTable: map[string][]string{"users": {"name"}},
			}),
		),
		localRegion:        "local",
		allowedPeerRegions: map[string]struct{}{"remote": {}},
	}
	stream := &fakeConsumeServer{ctx: context.Background()}

	err := handler.Consume(&peerv1.ConsumeRequest{
		Region:        "remote",
		Version:       "v1",
		Configuration: cfg,
		SnapshotRequests: []*peerv1.SnapshotRequest{
			{Table: "users"},
		},
	}, stream)
	if err != nil {
		t.Fatalf("Consume returned error: %v", err)
	}
	if len(stream.sent) != 3 {
		t.Fatalf(
			"expected handshake, one snapshot chunk, and snapshot complete, got %d messages",
			len(stream.sent),
		)
	}
	row := stream.sent[1].GetSnapshotChunk().GetRows()[0]
	if got := stream.sent[1].GetSnapshotChunk().GetWatermark().AsTime(); !got.Equal(time.Unix(10, 0).UTC()) {
		t.Fatalf("unexpected snapshot watermark: %v", got)
	}
	if _, ok := row.GetFields()["name"]; ok {
		t.Fatal("expected encrypted snapshot field to be removed from wire fields")
	}
	if got := string(row.GetMetadata()[transformers.TransitKey("name")]); got != `"alice"` {
		t.Fatalf("expected outbound snapshot pipeline to move decrypted field into metadata, got %s", got)
	}
	if stream.sent[2].GetSnapshotComplete().GetTable() != "users" {
		t.Fatalf("expected snapshot complete for users, got %#v", stream.sent[2])
	}
}

func TestConsumeReplaysPendingAfterSnapshot(t *testing.T) {
	t.Parallel()

	cfg := models.BuildHandshakeConfiguration("cdc", nil, nil)
	service := &fakeServerReplicationService{
		handshake: models.HandshakeMessage{Region: "local", Version: "v1", Configuration: cfg},
		sendSnapshotFn: func(
			_ context.Context,
			_ string,
			_ string,
			_ []models.PrimaryKeyPart,
			send func(models.SnapshotChunkMessage) error,
		) error {
			return send(models.SnapshotChunkMessage{
				Table: "users",
				Rows: []models.SnapshotRow{
					{
						PrimaryKey: []models.PrimaryKeyPart{{Column: "id", Type: models.PrimaryKeyTypeString, Value: []byte("1")}},
						Payload:    mustPayload(t, map[string][]byte{"id": []byte(`"1"`)}),
					},
				},
				Last: true,
			})
		},
	}
	handler := &Handler{
		service: service,
		outbox: &fakeAckOutbox{
			live: closedOutbox(),
			pending: []models.OutboxBatch{
				{
					ID:         1,
					PeerRegion: "remote",
					Table:      "users",
					Sequence:   1,
					CreatedAt:  time.Unix(9, 0).UTC(),
					Events: []models.ReplicationEvent{
						{
							PeerRegion: "local",
							Table:      "users",
							EventID:    "evt-1",
							PrimaryKey: []models.PrimaryKeyPart{{Column: "id", Type: models.PrimaryKeyTypeString, Value: []byte("1")}},
							Fields:     map[string][]byte{"id": []byte(`"1"`)},
						},
					},
				},
			},
		},
		pipeline:           transformers.NewOutbound(),
		localRegion:        "local",
		allowedPeerRegions: map[string]struct{}{"remote": {}},
	}
	stream := &fakeConsumeServer{ctx: context.Background()}

	err := handler.Consume(&peerv1.ConsumeRequest{
		Region:        "remote",
		Version:       "v1",
		Configuration: cfg,
		SnapshotRequests: []*peerv1.SnapshotRequest{
			{Table: "users"},
		},
	}, stream)
	if err != nil {
		t.Fatalf("Consume returned error: %v", err)
	}
	if len(stream.sent) != 4 {
		t.Fatalf(
			"expected handshake, snapshot chunk, pending batch, and snapshot complete, got %d messages",
			len(stream.sent),
		)
	}
	if stream.sent[1].GetSnapshotChunk() == nil {
		t.Fatalf("expected snapshot chunk second, got %#v", stream.sent[1])
	}
	if stream.sent[2].GetEventBatch() == nil {
		t.Fatalf("expected pending event batch after snapshot, got %#v", stream.sent[2])
	}
	if stream.sent[3].GetSnapshotComplete() == nil {
		t.Fatalf("expected snapshot complete after catch-up, got %#v", stream.sent[3])
	}
}

func TestConsumeReplaysFullPendingBacklogWithoutSnapshot(t *testing.T) {
	t.Parallel()

	cfg := models.BuildHandshakeConfiguration("cdc", nil, nil)
	pending := make([]models.OutboxBatch, 0, 1100)
	for i := 1; i <= 1100; i++ {
		pending = append(pending, models.OutboxBatch{
			ID:         uint64(i),
			PeerRegion: "remote",
			Table:      "users",
			Sequence:   uint64(i),
			CreatedAt:  time.Unix(int64(i), 0).UTC(),
			Events: []models.ReplicationEvent{
				{
					PeerRegion: "local",
					Table:      "users",
					EventID:    "evt-" + time.Unix(int64(i), 0).UTC().Format(time.RFC3339Nano),
					PrimaryKey: []models.PrimaryKeyPart{{Column: "id", Type: models.PrimaryKeyTypeString, Value: []byte("1")}},
					Fields:     map[string][]byte{"id": []byte(`"1"`)},
				},
			},
		})
	}

	handler := &Handler{
		service: &fakeServerReplicationService{
			handshake: models.HandshakeMessage{Region: "local", Version: "v1", Configuration: cfg},
		},
		outbox: &fakeAckOutbox{
			live:      closedOutbox(),
			pending:   pending,
			watermark: time.Unix(2000, 0).UTC(),
		},
		pipeline:           transformers.NewOutbound(),
		localRegion:        "local",
		allowedPeerRegions: map[string]struct{}{"remote": {}},
	}
	stream := &fakeConsumeServer{ctx: context.Background()}

	err := handler.Consume(&peerv1.ConsumeRequest{
		Region:        "remote",
		Version:       "v1",
		Configuration: cfg,
	}, stream)
	if err != nil {
		t.Fatalf("Consume returned error: %v", err)
	}

	if len(stream.sent) != 1+len(pending) {
		t.Fatalf("expected handshake and %d pending batches, got %d messages", len(pending), len(stream.sent))
	}
	if stream.sent[0].GetHandshake() == nil {
		t.Fatalf("expected handshake first, got %#v", stream.sent[0])
	}
	if stream.sent[len(stream.sent)-1].GetEventBatch() == nil {
		t.Fatalf("expected event batch last, got %#v", stream.sent[len(stream.sent)-1])
	}
}

func TestConsumeReplaysPendingBatchesWithDuplicateTableSequences(t *testing.T) {
	t.Parallel()

	cfg := models.BuildHandshakeConfiguration("cdc", nil, nil)
	pending := make([]models.OutboxBatch, 0, replayPendingPageSize+3)
	for i := 1; i <= replayPendingPageSize+3; i++ {
		table := fmt.Sprintf("table_%03d", i)
		pending = append(pending, models.OutboxBatch{
			ID:         uint64(i),
			PeerRegion: "remote",
			Table:      table,
			Sequence:   1,
			CreatedAt:  time.Unix(int64(i), 0).UTC(),
			Events: []models.ReplicationEvent{
				{
					PeerRegion: "local",
					Table:      table,
					EventID:    fmt.Sprintf("evt-%03d", i),
					PrimaryKey: []models.PrimaryKeyPart{{Column: "id", Type: models.PrimaryKeyTypeString, Value: []byte("1")}},
					Fields:     map[string][]byte{"id": []byte(`"1"`)},
				},
			},
		})
	}

	handler := &Handler{
		service: &fakeServerReplicationService{
			handshake: models.HandshakeMessage{Region: "local", Version: "v1", Configuration: cfg},
		},
		outbox: &fakeAckOutbox{
			live:      closedOutbox(),
			pending:   pending,
			watermark: time.Unix(2000, 0).UTC(),
		},
		pipeline:           transformers.NewOutbound(),
		localRegion:        "local",
		allowedPeerRegions: map[string]struct{}{"remote": {}},
	}
	stream := &fakeConsumeServer{ctx: context.Background()}

	err := handler.Consume(&peerv1.ConsumeRequest{
		Region:        "remote",
		Version:       "v1",
		Configuration: cfg,
	}, stream)
	if err != nil {
		t.Fatalf("Consume returned error: %v", err)
	}

	if len(stream.sent) != 1+len(pending) {
		t.Fatalf("expected handshake and %d pending batches, got %d messages", len(pending), len(stream.sent))
	}
	if got := stream.sent[len(stream.sent)-1].GetEventBatch().GetEvents()[0].GetEventId(); got != "evt-259" {
		t.Fatalf("expected final duplicate-sequence batch to be replayed, got %s", got)
	}
}

func TestConsumePassesSnapshotResumeToken(t *testing.T) {
	t.Parallel()

	cfg := models.BuildHandshakeConfiguration("cdc", nil, nil)
	var gotAfterPrimaryKey []models.PrimaryKeyPart
	service := &fakeServerReplicationService{
		handshake: models.HandshakeMessage{Region: "local", Version: "v1", Configuration: cfg},
		sendSnapshotFn: func(
			_ context.Context,
			_ string,
			_ string,
			afterPrimaryKey []models.PrimaryKeyPart,
			send func(models.SnapshotChunkMessage) error,
		) error {
			gotAfterPrimaryKey = models.ClonePrimaryKeyParts(afterPrimaryKey)
			return send(models.SnapshotChunkMessage{Table: "users", Last: true})
		},
	}
	handler := &Handler{
		service:            service,
		outbox:             &fakeAckOutbox{live: closedOutbox()},
		pipeline:           transformers.NewOutbound(),
		localRegion:        "local",
		allowedPeerRegions: map[string]struct{}{"remote": {}},
	}

	err := handler.Consume(&peerv1.ConsumeRequest{
		Region:        "remote",
		Version:       "v1",
		Configuration: cfg,
		SnapshotRequests: []*peerv1.SnapshotRequest{
			{
				Table: "users",
				AfterPrimaryKey: []*peerv1.PrimaryKeyPart{
					{Column: "id", Value: &peerv1.PrimaryKeyPart_StringValue{StringValue: "pk-7"}},
				},
			},
		},
	}, &fakeConsumeServer{ctx: context.Background()})
	if err != nil {
		t.Fatalf("Consume returned error: %v", err)
	}
	if len(gotAfterPrimaryKey) != 1 {
		t.Fatalf("expected one resume key part, got %#v", gotAfterPrimaryKey)
	}
	if got := string(gotAfterPrimaryKey[0].Value); got != `"pk-7"` {
		t.Fatalf("unexpected resume key payload: %s", got)
	}
}

type fakeServerReplicationService struct {
	handshake      models.HandshakeMessage
	sendSnapshotFn func(
		context.Context,
		string,
		string,
		[]models.PrimaryKeyPart,
		func(models.SnapshotChunkMessage) error,
	) error
}

func (f *fakeServerReplicationService) Handshake(context.Context) models.HandshakeMessage {
	return f.handshake
}

func (f *fakeServerReplicationService) SendSnapshotToPeer(
	ctx context.Context,
	peerRegion, table string,
	afterPrimaryKey []models.PrimaryKeyPart,
	send func(models.SnapshotChunkMessage) error,
) error {
	if f.sendSnapshotFn != nil {
		return f.sendSnapshotFn(ctx, peerRegion, table, afterPrimaryKey, send)
	}
	return nil
}

func (f *fakeServerReplicationService) RecordMsgSent(int)     {}
func (f *fakeServerReplicationService) RecordMsgReceived(int) {}

type fakeAckOutbox struct {
	live        <-chan models.OutboxBatch
	pending     []models.OutboxBatch
	watermark   time.Time
	peerRegion  string
	table       string
	lastEventID string
}

func (f *fakeAckOutbox) Subscribe(context.Context, string) (<-chan models.OutboxBatch, func(), error) {
	return f.live, func() {}, nil
}

func (f *fakeAckOutbox) ListPending(context.Context, string, int) ([]models.OutboxBatch, error) {
	return f.pending, nil
}

func (f *fakeAckOutbox) ListPendingWindow(
	_ context.Context,
	_ string,
	lowerExclusive, upperInclusive time.Time,
	afterID uint64,
	limit int,
) ([]models.OutboxBatch, error) {
	var pending []models.OutboxBatch
	for _, batch := range f.pending {
		if batch.ID <= afterID {
			continue
		}
		if !withinWindow(batch.CreatedAt, lowerExclusive, upperInclusive) {
			continue
		}
		pending = append(pending, batch)
		if limit > 0 && len(pending) == limit {
			break
		}
	}
	return pending, nil
}

func (f *fakeAckOutbox) Ack(_ context.Context, peerRegion, table, lastEventID string) (bool, error) {
	f.peerRegion = peerRegion
	f.table = table
	f.lastEventID = lastEventID
	return true, nil
}

func (f *fakeAckOutbox) SnapshotWatermark(context.Context) (time.Time, error) {
	if f.watermark.IsZero() {
		return time.Unix(10, 0).UTC(), nil
	}
	return f.watermark, nil
}

type fakeConsumeServer struct {
	ctx  context.Context
	sent []*peerv1.ConsumeResponse
}

func (s *fakeConsumeServer) Send(msg *peerv1.ConsumeResponse) error {
	s.sent = append(s.sent, msg)
	return nil
}
func (s *fakeConsumeServer) SetHeader(metadata.MD) error  { return nil }
func (s *fakeConsumeServer) SendHeader(metadata.MD) error { return nil }
func (s *fakeConsumeServer) SetTrailer(metadata.MD)       {}
func (s *fakeConsumeServer) Context() context.Context     { return s.ctx }
func (s *fakeConsumeServer) SendMsg(m any) error {
	msg, ok := m.(*peerv1.ConsumeResponse)
	if !ok {
		return io.ErrUnexpectedEOF
	}
	return s.Send(msg)
}
func (s *fakeConsumeServer) RecvMsg(any) error { return io.EOF }

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

func mustPayload(t *testing.T, fields map[string][]byte) []byte {
	t.Helper()
	payload, err := models.FieldsToPayload(fields)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	return payload
}

func closedOutbox() <-chan models.OutboxBatch {
	ch := make(chan models.OutboxBatch)
	close(ch)
	return ch
}
