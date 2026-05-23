package client

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/transmogr/transmogr/internal/models"
	transformers "github.com/transmogr/transmogr/internal/replication/transformers"
)

type (
	// peersService lists peers this region may consume from.
	peersService interface {
		// Peers returns the peers currently eligible for consume sessions.
		Peers() []models.Peer
	}

	// leaseService persists consume-session lease ownership.
	leaseService interface {
		// TryStateAcquireLease attempts to acquire the requested stream lease.
		TryStateAcquireLease(
			ctx context.Context,
			kind string,
			region string,
			peerRegion string,
			table string,
			ownerID string,
			ttl time.Duration,
		) (models.Lease, bool, error)
		// RenewStateLease extends a lease still owned by this instance.
		RenewStateLease(
			ctx context.Context,
			kind string,
			region string,
			peerRegion string,
			table string,
			ownerID string,
			generation int64,
			ttl time.Duration,
		) (models.Lease, bool, error)
		// ReleaseStateLease releases a lease still owned by this instance.
		ReleaseStateLease(
			ctx context.Context,
			kind string,
			region string,
			peerRegion string,
			table string,
			ownerID string,
			generation int64,
		) (bool, error)
	}

	// replicationService exposes the replication operations needed by the gRPC consume client.
	replicationService interface {
		// Handshake builds the local replication handshake payload.
		Handshake(ctx context.Context) models.HandshakeMessage
		// RecordReconnectAttempt records the reconnect delay selected for a failed session.
		RecordReconnectAttempt(delay time.Duration)
		// ApplySnapshotChunk applies one inbound snapshot chunk.
		ApplySnapshotChunk(
			ctx context.Context,
			peerRegion, table string,
			rows []models.SnapshotRow,
			last bool,
			watermark time.Time,
		) error
		// CompleteSnapshot marks a snapshot bootstrap as fully caught up and complete.
		CompleteSnapshot(ctx context.Context, peerRegion, table string) error
		// ApplyEventBatch applies one inbound polling event batch without a wrapping transaction.
		ApplyEventBatch(ctx context.Context, events []models.ReplicationEvent) error
		// ApplyTransactionBatch applies one inbound CDC transaction batch atomically.
		ApplyTransactionBatch(ctx context.Context, events []models.ReplicationEvent) error
		// QueueSnapshotRequests calls send for each table that needs a snapshot from the given peer.
		QueueSnapshotRequests(ctx context.Context, peerRegion string, send func(models.SnapshotRequest) error) error
		// RecordMsgSent records one outbound gRPC message by type.
		RecordMsgSent(msgType int)
		// RecordMsgReceived records one inbound gRPC message by type.
		RecordMsgReceived(msgType int)
	}
)

// Manager maintains outbound consume sessions to all configured peers.
type Manager struct {
	localRegion        string
	instanceID         string
	peers              peersService
	replication        replicationService
	state              leaseService
	pipeline           transformers.Inbound
	reconnectDelay     time.Duration
	pingInterval       time.Duration
	leaseTTL           time.Duration
	leaseRenewInterval time.Duration
	transportCreds     credentials.TransportCredentials
	backoff            *backoff

	mu    sync.Mutex
	conns []*grpc.ClientConn
}

// Config controls reconnect and distributed lease ownership for consume sessions.
type Config struct {
	LocalRegion          string
	InstanceID           string
	ReconnectDelay       time.Duration
	PingInterval         time.Duration
	LeaseTTL             time.Duration
	LeaseRenewInterval   time.Duration
	TransportCredentials credentials.TransportCredentials
}

// NewManager creates the gRPC consume client manager.
func NewManager(
	cfg Config,
	peers peersService,
	replication replicationService,
	stateRepo leaseService,
	pipe transformers.Inbound,
) *Manager {
	return &Manager{
		localRegion:        cfg.LocalRegion,
		instanceID:         cfg.InstanceID,
		peers:              peers,
		replication:        replication,
		state:              stateRepo,
		pipeline:           pipe,
		reconnectDelay:     cfg.ReconnectDelay,
		pingInterval:       cfg.PingInterval,
		leaseTTL:           cfg.LeaseTTL,
		leaseRenewInterval: cfg.LeaseRenewInterval,
		transportCreds:     cfg.TransportCredentials,
		backoff:            newBackoff(cfg.ReconnectDelay),
	}
}

// Run starts one reconnecting stream loop per peer and blocks until shutdown.
func (m *Manager) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, peerConfig := range m.peers.Peers() {
		wg.Add(1)
		go func(peerConfig models.Peer) {
			defer wg.Done()
			m.runPeerLoop(ctx, peerConfig)
		}(peerConfig)
	}

	<-ctx.Done()
	wg.Wait()
	return nil
}

// Close closes all active gRPC client connections.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var err error
	for _, conn := range m.conns {
		err = errors.Join(err, conn.Close())
	}

	m.conns = nil
	return err
}

func (m *Manager) addConn(conn *grpc.ClientConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conns = append(m.conns, conn)
}

func (m *Manager) removeConn(conn *grpc.ClientConn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, candidate := range m.conns {
		if candidate == conn {
			m.conns = append(m.conns[:i], m.conns[i+1:]...)
			return
		}
	}
}
