package server

import (
	grpcprotocol "github.com/transmogr/transmogr/internal/transport/grpc/protocol"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

// sendLoop drains the served-feed queue and writes each message to the consume stream.
// It returns when the queue is closed or a send fails.
func (h *Handler) sendLoop(
	stream peerv1.ReplicationService_ConsumeServer,
	sendQueue <-chan *peerv1.ConsumeResponse,
) error {
	for message := range sendQueue {
		if message == nil {
			continue
		}
		if err := stream.Send(message); err != nil {
			return err
		}
		if h.service != nil {
			h.service.RecordMsgSent(grpcprotocol.MessageMetricType(message))
		}
	}
	return nil
}
