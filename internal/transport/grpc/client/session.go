package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"google.golang.org/grpc"

	"github.com/transmogr/transmogr/internal/models"
	grpcprotocol "github.com/transmogr/transmogr/internal/transport/grpc/protocol"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

// runPeerSession opens a gRPC consume stream to one peer region and applies the
// server-emitted replication feed until the session ends or fails.
func (m *Manager) runPeerSession(ctx context.Context, peerConfig models.Peer) error {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	slog.Info("peer consume session connecting",
		"local_region", m.localRegion,
		"peer_region", peerConfig.Region,
		"endpoint", peerConfig.Endpoint,
	)

	conn, err := grpc.NewClient(peerConfig.Endpoint, grpc.WithTransportCredentials(m.transportCreds))
	if err != nil {
		return fmt.Errorf("dial peer %s: %w", peerConfig.Region, err)
	}
	m.addConn(conn)
	defer func() {
		m.removeConn(conn)
		_ = conn.Close()
	}()

	client := peerv1.NewReplicationServiceClient(conn)

	localHandshake := m.replication.Handshake(ctx)
	snapshotRequests := make([]models.SnapshotRequest, 0, 8)
	if err := m.replication.QueueSnapshotRequests(
		sessionCtx,
		peerConfig.Region,
		func(request models.SnapshotRequest) error {
			snapshotRequests = append(snapshotRequests, request)
			return nil
		},
	); err != nil {
		return err
	}

	request := grpcprotocol.NewConsumeRequest(localHandshake, snapshotRequests)
	m.replication.RecordMsgSent(grpcprotocol.MessageMetricType(request))

	stream, err := client.Consume(sessionCtx, request)
	if err != nil {
		return fmt.Errorf("open consume stream to %s: %w", peerConfig.Region, err)
	}
	slog.Info("peer consume session connected",
		"local_region", m.localRegion,
		"peer_region", peerConfig.Region,
	)

	err = m.recvLoop(sessionCtx, peerConfig.Region, localHandshake, client, stream)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
