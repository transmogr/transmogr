package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/transmogr/transmogr/internal/models"
	transformers "github.com/transmogr/transmogr/internal/replication/transformers"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

// Server owns the gRPC transport listener and replication service registration.
type Server struct {
	addr string
	svc  replicationService
	ob   ackOutbox
	pipe transformers.Outbound

	mu       sync.Mutex
	listener net.Listener
	server   *grpc.Server
}

// Config controls gRPC server transport settings.
type Config struct {
	Addr                 string
	LocalRegion          string
	AllowedPeers         []models.Peer
	TransportCredentials credentials.TransportCredentials
}

// New constructs the gRPC server transport.
func New(cfg Config, svc replicationService, ob ackOutbox, pipe transformers.Outbound) (*Server, error) {
	if cfg.Addr == "" {
		return nil, errors.New("grpc listen address is required")
	}

	serverOptions := make([]grpc.ServerOption, 0, 1)
	if cfg.TransportCredentials != nil {
		serverOptions = append(serverOptions, grpc.Creds(cfg.TransportCredentials))
	}

	grpcServer := grpc.NewServer(serverOptions...)
	peerv1.RegisterReplicationServiceServer(grpcServer, &Handler{
		service:            svc,
		outbox:             ob,
		pipeline:           pipe,
		localRegion:        cfg.LocalRegion,
		allowedPeerRegions: allowedPeerRegionSet(cfg.AllowedPeers),
	})

	return &Server{
		addr:   cfg.Addr,
		svc:    svc,
		ob:     ob,
		pipe:   pipe,
		server: grpcServer,
	}, nil
}

// Run starts serving the gRPC transport until shutdown.
func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.server.GracefulStop()
	}()

	if err := s.server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("serve grpc: %w", err)
	}

	return nil
}

func allowedPeerRegionSet(peers []models.Peer) map[string]struct{} {
	if len(peers) == 0 {
		return nil
	}

	allowed := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		if peer.Region == "" {
			continue
		}
		allowed[peer.Region] = struct{}{}
	}

	return allowed
}

// Close stops the gRPC server and closes the listener.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.server != nil {
		s.server.GracefulStop()
	}

	if s.listener != nil {
		return s.listener.Close()
	}

	return nil
}
