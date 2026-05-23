package client

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/transmogr/transmogr/internal/models"
	transformers "github.com/transmogr/transmogr/internal/replication/transformers"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

func TestRecvLoopAppliesEventBatchAndAcks(t *testing.T) {
	t.Parallel()

	cfg := models.BuildHandshakeConfiguration("cdc", nil, nil)
	replication := &fakeReplicationService{}
	manager := &Manager{
		localRegion: "local",
		replication: replication,
		pipeline: transformers.NewInbound(
			transformers.NewCryptoInboundTransformer(fakeCryptoService{
				encryptFn: func(_ context.Context, _ string, fields map[string][]byte) (map[string][]byte, error) {
					projected := models.CloneFields(fields)
					projected["name"] = []byte(`"ciphertext"`)
					return projected, nil
				},
				fieldsByTable: map[string][]string{"users": {"name"}},
			}),
			transformers.NewInboundMetadataTransformer(),
		),
	}
	client := &fakeReplicationClient{}
	stream := &fakeConsumeClient{
		ctx: context.Background(),
		recv: []*peerv1.ConsumeResponse{
			{
				Msg: &peerv1.ConsumeResponse_Handshake{
					Handshake: &peerv1.Handshake{Region: "remote", Version: "v1", Configuration: cfg},
				},
			},
			{
				Msg: &peerv1.ConsumeResponse_EventBatch{
					EventBatch: &peerv1.EventBatch{
						Events: []*peerv1.Event{
							{
								EventId:      "evt-1",
								OriginRegion: "remote",
								Table:        "users",
								PrimaryKey: []*peerv1.PrimaryKeyPart{
									{
										Column: "id",
										Value: &peerv1.PrimaryKeyPart_StringValue{
											StringValue: "1",
										},
									},
								},
								Metadata: map[string][]byte{
									"trace.request_id":              []byte(`"req-1"`),
									"transit.virtual.full_name":     []byte(`"Alice Smith"`),
									transformers.TransitKey("name"): []byte(`"alice"`),
								},
							},
						},
					},
				},
			},
		},
	}

	err := manager.recvLoop(
		context.Background(),
		"remote",
		models.HandshakeMessage{Version: "v1", Configuration: cfg},
		client,
		stream,
	)
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if len(replication.appliedEvents) != 1 {
		t.Fatalf("expected one applied batch, got %d", len(replication.appliedEvents))
	}
	if got := string(replication.appliedEvents[0][0].Fields["name"]); got != `"ciphertext"` {
		t.Fatalf("expected inbound pipeline to project fields, got %s", got)
	}
	if got := string(replication.appliedEvents[0][0].Metadata["trace.request_id"]); got != `"req-1"` {
		t.Fatalf("expected regular metadata to be preserved, got %s", got)
	}
	if _, ok := replication.appliedEvents[0][0].Metadata[transformers.TransitKey("name")]; ok {
		t.Fatal("expected crypto transit metadata to be consumed by inbound transformer")
	}
	if _, ok := replication.appliedEvents[0][0].Metadata["transit.virtual.full_name"]; ok {
		t.Fatal("expected leftover transit metadata to be cleaned up")
	}
	if client.lastAck == nil ||
		client.lastAck.GetRegion() != "local" ||
		client.lastAck.GetTable() != "users" ||
		client.lastAck.GetLastEventId() != "evt-1" {
		t.Fatalf("unexpected ack: %#v", client.lastAck)
	}
}

type fakeReplicationService struct {
	appliedEvents    [][]models.ReplicationEvent
	appliedSnapshots [][]models.SnapshotRow
	completedTables  []string
}

func (f *fakeReplicationService) Handshake(context.Context) models.HandshakeMessage {
	return models.HandshakeMessage{}
}

func (f *fakeReplicationService) RecordReconnectAttempt(time.Duration) {}

func (f *fakeReplicationService) ApplySnapshotChunk(
	_ context.Context,
	_ string,
	_ string,
	rows []models.SnapshotRow,
	_ bool,
	_ time.Time,
) error {
	f.appliedSnapshots = append(f.appliedSnapshots, rows)
	return nil
}

func (f *fakeReplicationService) ApplyEventBatch(_ context.Context, events []models.ReplicationEvent) error {
	f.appliedEvents = append(f.appliedEvents, events)
	return nil
}

func (f *fakeReplicationService) ApplyTransactionBatch(context.Context, []models.ReplicationEvent) error {
	return nil
}

func (f *fakeReplicationService) QueueSnapshotRequests(
	context.Context,
	string,
	func(models.SnapshotRequest) error,
) error {
	return nil
}

func (f *fakeReplicationService) CompleteSnapshot(_ context.Context, _ string, table string) error {
	f.completedTables = append(f.completedTables, table)
	return nil
}

func (f *fakeReplicationService) RecordMsgSent(int)     {}
func (f *fakeReplicationService) RecordMsgReceived(int) {}

type fakeReplicationClient struct {
	lastAck *peerv1.AckRequest
}

func (f *fakeReplicationClient) Consume(
	context.Context,
	*peerv1.ConsumeRequest,
	...grpc.CallOption,
) (grpc.ServerStreamingClient[peerv1.ConsumeResponse], error) {
	return nil, nil
}

func (f *fakeReplicationClient) Ack(
	_ context.Context,
	in *peerv1.AckRequest,
	_ ...grpc.CallOption,
) (*peerv1.AckResponse, error) {
	f.lastAck = in
	return &peerv1.AckResponse{}, nil
}

type fakeConsumeClient struct {
	ctx  context.Context
	recv []*peerv1.ConsumeResponse
}

func (c *fakeConsumeClient) Header() (metadata.MD, error) { return nil, nil }
func (c *fakeConsumeClient) Trailer() metadata.MD         { return nil }
func (c *fakeConsumeClient) CloseSend() error             { return nil }
func (c *fakeConsumeClient) Context() context.Context     { return c.ctx }
func (c *fakeConsumeClient) SendMsg(any) error            { return nil }
func (c *fakeConsumeClient) RecvMsg(any) error            { return nil }
func (c *fakeConsumeClient) Recv() (*peerv1.ConsumeResponse, error) {
	if len(c.recv) == 0 {
		return nil, io.EOF
	}
	msg := c.recv[0]
	c.recv = c.recv[1:]
	return msg, nil
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

func TestRecvLoopAppliesSnapshotChunkThroughPipeline(t *testing.T) {
	t.Parallel()

	cfg := models.BuildHandshakeConfiguration("cdc", nil, nil)
	replication := &fakeReplicationService{}
	manager := &Manager{
		localRegion: "local",
		replication: replication,
		pipeline: transformers.NewInbound(
			transformers.NewCryptoInboundTransformer(fakeCryptoService{
				encryptFn: func(_ context.Context, _ string, fields map[string][]byte) (map[string][]byte, error) {
					projected := models.CloneFields(fields)
					projected["name"] = []byte(`"ciphertext"`)
					return projected, nil
				},
				fieldsByTable: map[string][]string{"users": {"name"}},
			}),
			transformers.NewInboundMetadataTransformer(),
		),
	}

	stream := &fakeConsumeClient{
		ctx: context.Background(),
		recv: []*peerv1.ConsumeResponse{
			{
				Msg: &peerv1.ConsumeResponse_Handshake{
					Handshake: &peerv1.Handshake{Region: "remote", Version: "v1", Configuration: cfg},
				},
			},
			{
				Msg: &peerv1.ConsumeResponse_SnapshotChunk{
					SnapshotChunk: &peerv1.SnapshotChunk{
						Table: "users",
						Rows: []*peerv1.SnapshotRow{
							{
								PrimaryKey: []*peerv1.PrimaryKeyPart{
									{
										Column: "id",
										Value: &peerv1.PrimaryKeyPart_StringValue{
											StringValue: "1",
										},
									},
								},
								Metadata: map[string][]byte{
									"trace.request_id":              []byte(`"req-1"`),
									"transit.virtual.full_name":     []byte(`"Alice Smith"`),
									transformers.TransitKey("name"): []byte(`"alice"`),
								},
							},
						},
					},
				},
			},
		},
	}

	err := manager.recvLoop(
		context.Background(),
		"remote",
		models.HandshakeMessage{Version: "v1", Configuration: cfg},
		&fakeReplicationClient{},
		stream,
	)
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if len(replication.appliedSnapshots) != 1 {
		t.Fatalf("expected one applied snapshot batch, got %d", len(replication.appliedSnapshots))
	}
	fields, err := models.PayloadToFields(replication.appliedSnapshots[0][0].Payload)
	if err != nil {
		t.Fatalf("decode applied snapshot payload: %v", err)
	}
	if got := string(fields["name"]); got != `"ciphertext"` {
		t.Fatalf("expected inbound snapshot pipeline to project fields, got %s", got)
	}
	if got := string(replication.appliedSnapshots[0][0].Metadata["trace.request_id"]); got != `"req-1"` {
		t.Fatalf("expected regular snapshot metadata to be preserved, got %s", got)
	}
	if _, ok := replication.appliedSnapshots[0][0].Metadata[transformers.TransitKey("name")]; ok {
		t.Fatal("expected snapshot crypto transit metadata to be consumed")
	}
	if _, ok := replication.appliedSnapshots[0][0].Metadata["transit.virtual.full_name"]; ok {
		t.Fatal("expected leftover snapshot transit metadata to be cleaned up")
	}
}
