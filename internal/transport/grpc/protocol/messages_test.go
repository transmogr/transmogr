package protocol

import (
	"testing"
	"time"

	"github.com/transmogr/transmogr/internal/models"
	"github.com/transmogr/transmogr/internal/service/replication"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

func TestNewConsumeRequest(t *testing.T) {
	t.Parallel()

	msg := NewConsumeRequest(models.HandshakeMessage{
		Region:        "eu",
		Version:       "v1",
		Configuration: "cfg",
	}, []models.SnapshotRequest{
		{
			Table:           "users",
			AfterPrimaryKey: models.PrimaryKeyString("id", "42"),
		},
	})

	if msg.GetRegion() != "eu" || msg.GetVersion() != "v1" || msg.GetConfiguration() != "cfg" {
		t.Fatalf("unexpected request: %#v", msg)
	}
	if len(msg.GetSnapshotRequests()) != 1 {
		t.Fatalf("unexpected snapshot request payload: %#v", msg)
	}
	if len(msg.GetSnapshotRequests()) != 1 || msg.GetSnapshotRequests()[0].GetTable() != "users" {
		t.Fatalf("unexpected snapshot request details: %#v", msg)
	}
	if got := msg.GetSnapshotRequests()[0].GetAfterPrimaryKey()[0].GetStringValue(); got != "42" {
		t.Fatalf("unexpected snapshot resume key: %q", got)
	}
}

func TestSnapshotRequestsFromConsume(t *testing.T) {
	t.Parallel()

	requests := SnapshotRequestsFromConsume(&peerv1.ConsumeRequest{
		SnapshotRequests: []*peerv1.SnapshotRequest{
			{
				Table: "users",
				AfterPrimaryKey: []*peerv1.PrimaryKeyPart{
					{Column: "id", Value: &peerv1.PrimaryKeyPart_StringValue{StringValue: "42"}},
				},
			},
		},
	})
	if len(requests) != 1 || requests[0].Table != "users" {
		t.Fatalf("unexpected snapshot requests: %#v", requests)
	}
	if got := string(requests[0].AfterPrimaryKey[0].Value); got != `"42"` {
		t.Fatalf("unexpected decoded snapshot resume key: %s", got)
	}
}

func TestNewAckRequest(t *testing.T) {
	t.Parallel()

	msg := NewAckRequest("eu", "users", "evt-1")
	if msg.GetRegion() != "eu" || msg.GetTable() != "users" || msg.GetLastEventId() != "evt-1" {
		t.Fatalf("unexpected ack request: %#v", msg)
	}
}

func TestNewHeartbeatMessage(t *testing.T) {
	t.Parallel()

	now := time.Unix(1, 0)
	msg := NewHeartbeatMessage(now)
	if got := msg.GetHeartbeat().GetSentAt().AsTime(); !got.Equal(now) {
		t.Fatalf("unexpected heartbeat time: %v", got)
	}
}

func TestReplicationEventsMetadataRoundTrip(t *testing.T) {
	t.Parallel()

	events := []models.ReplicationEvent{
		{
			PeerRegion: "eu",
			Table:      "users",
			EventID:    "evt-1",
			PrimaryKey: []models.PrimaryKeyPart{
				{Column: "id", Type: models.PrimaryKeyTypeString, Value: []byte(`"1"`)},
			},
			Fields: map[string][]byte{
				"name": []byte(`"alice"`),
			},
			Metadata: map[string][]byte{
				"transit.virtual.full_name": []byte(`"Alice Smith"`),
				"trace.request_id":          []byte(`"req-1"`),
			},
		},
	}

	protoEvents := toProtoEvents(events)
	decoded, err := ToReplicationEvents(protoEvents)
	if err != nil {
		t.Fatalf("decode events: %v", err)
	}

	if got := string(decoded[0].Metadata["transit.virtual.full_name"]); got != `"Alice Smith"` {
		t.Fatalf("unexpected metadata virtual field: %s", got)
	}
	if got := string(decoded[0].Metadata["trace.request_id"]); got != `"req-1"` {
		t.Fatalf("unexpected metadata trace field: %s", got)
	}
}

func TestSnapshotRowsMetadataRoundTrip(t *testing.T) {
	t.Parallel()

	payload, err := models.FieldsToPayload(map[string][]byte{
		"name": []byte(`"alice"`),
	})
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}

	rows := []models.SnapshotRow{
		{
			PrimaryKey: []models.PrimaryKeyPart{
				{Column: "id", Type: models.PrimaryKeyTypeString, Value: []byte(`"1"`)},
			},
			Payload: payload,
			Metadata: map[string][]byte{
				"transit.virtual.full_name": []byte(`"Alice Smith"`),
				"trace.request_id":          []byte(`"req-1"`),
			},
		},
	}

	protoRows := toProtoSnapshotRows(rows)
	decoded, err := ToSnapshotRows(protoRows)
	if err != nil {
		t.Fatalf("decode rows: %v", err)
	}
	if got := string(decoded[0].Metadata["transit.virtual.full_name"]); got != `"Alice Smith"` {
		t.Fatalf("unexpected snapshot metadata virtual field: %s", got)
	}
	if got := string(decoded[0].Metadata["trace.request_id"]); got != `"req-1"` {
		t.Fatalf("unexpected snapshot metadata trace field: %s", got)
	}
}

func TestMessageMetricType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		msg  any
		want int
	}{
		{
			name: "consume",
			msg:  NewConsumeRequest(models.HandshakeMessage{}, nil),
			want: replication.MsgTypeConsumeRequest,
		},
		{name: "ack", msg: NewAckRequest("eu", "users", "evt-1"), want: replication.MsgTypeAckRequest},
		{name: "heartbeat", msg: NewHeartbeatMessage(time.Unix(1, 0)), want: replication.MsgTypeHeartbeat},
	}

	for _, tc := range cases {
		if got := MessageMetricType(tc.msg); got != tc.want {
			t.Fatalf("%s: got %d want %d", tc.name, got, tc.want)
		}
	}
}
