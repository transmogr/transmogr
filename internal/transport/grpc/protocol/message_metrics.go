package protocol

import (
	"github.com/transmogr/transmogr/internal/service/replication"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

// MessageMetricType maps a transport message to its metric type index.
func MessageMetricType(message any) int {
	if message == nil {
		return -1
	}

	switch payload := message.(type) {
	case *peerv1.ConsumeResponse:
		switch payload.GetMsg().(type) {
		case *peerv1.ConsumeResponse_Handshake:
			return replication.MsgTypeHandshake
		case *peerv1.ConsumeResponse_SnapshotChunk:
			return replication.MsgTypeSnapshotChunk
		case *peerv1.ConsumeResponse_SnapshotComplete:
			return replication.MsgTypeSnapshotComplete
		case *peerv1.ConsumeResponse_EventBatch:
			return replication.MsgTypeEventBatch
		case *peerv1.ConsumeResponse_TransactionBatch:
			return replication.MsgTypeTransactionBatch
		case *peerv1.ConsumeResponse_Heartbeat:
			return replication.MsgTypeHeartbeat
		default:
			return -1
		}
	case *peerv1.ConsumeRequest:
		return replication.MsgTypeConsumeRequest
	case *peerv1.AckRequest:
		return replication.MsgTypeAckRequest
	default:
		return -1
	}
}
