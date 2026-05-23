package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/transmogr/transmogr/internal/metrics"
	transformers "github.com/transmogr/transmogr/internal/replication/transformers"
	repositorypostgres "github.com/transmogr/transmogr/internal/repository/postgres"
	cryptoservice "github.com/transmogr/transmogr/internal/service/crypto"
	"github.com/transmogr/transmogr/internal/service/outbox"
	"github.com/transmogr/transmogr/internal/service/peers"
	"github.com/transmogr/transmogr/internal/service/replication"
	grpcclient "github.com/transmogr/transmogr/internal/transport/grpc/client"
	grpcserver "github.com/transmogr/transmogr/internal/transport/grpc/server"
)

func newGRPCTransportCredentials(
	grpcCfg GRPCTransportConfig,
) (credentials.TransportCredentials, credentials.TransportCredentials, error) {
	if grpcCfg.Insecure {
		return insecure.NewCredentials(), nil, nil
	}
	cfg := grpcCfg.TLS

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load grpc tls key pair: %w", err)
	}

	caPEM, err := os.ReadFile(cfg.ClientCAFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read grpc tls ca file: %w", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("parse grpc tls ca file %q", cfg.ClientCAFile)
	}

	serverTLS := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	}

	clientTLS := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		ServerName:   cfg.ServerName,
	}

	return credentials.NewTLS(clientTLS), credentials.NewTLS(serverTLS), nil
}

func (a *App) initTransport(
	instanceID string,
	km cryptoservice.KeyManager,
	cryptoSvc *cryptoservice.Service,
	repSvc *replication.Service,
	peerMgr *peers.Service,
	ob *outbox.Service,
	repo *repositorypostgres.Repository,
) error {
	clientTransportCreds, serverTransportCreds, err := newGRPCTransportCredentials(a.cfg.Transport.GRPC)
	if err != nil {
		return err
	}

	server, err := grpcserver.New(grpcserver.Config{
		Addr:                 a.cfg.Transport.GRPC.ListenAddr,
		LocalRegion:          a.cfg.Region,
		AllowedPeers:         peerMgr.Peers(),
		TransportCredentials: serverTransportCreds,
	}, repSvc, ob, transformers.NewOutbound(
		transformers.NewCryptoOutboundTransformer(cryptoSvc),
	))
	if err != nil {
		return fmt.Errorf("create grpc server: %w", err)
	}

	clientManager := grpcclient.NewManager(
		grpcclient.Config{
			LocalRegion:          a.cfg.Region,
			InstanceID:           instanceID,
			ReconnectDelay:       a.cfg.Transport.ReconnectDelay,
			PingInterval:         a.cfg.Transport.PingInterval,
			LeaseTTL:             a.cfg.Lease.TTL,
			LeaseRenewInterval:   a.cfg.Lease.RenewInterval,
			TransportCredentials: clientTransportCreds,
		},
		peerMgr,
		repSvc,
		repo,
		transformers.NewInbound(
			transformers.NewCryptoInboundTransformer(cryptoSvc),
			transformers.NewInboundMetadataTransformer(),
		),
	)

	a.runners = append(a.runners, server, clientManager)
	a.closers = append(a.closers, server, clientManager)

	if a.cfg.Transport.Metrics.ListenAddr != "" {
		var readiness metrics.ReadinessSource
		if ready, ok := km.(interface{ Ready(context.Context) error }); ok {
			readiness = ready
		}
		readiness = metrics.CombineReadiness(readiness, ob)
		metricsServer := metrics.NewServer(a.cfg.Transport.Metrics.ListenAddr, a.reg, readiness)
		a.runners = append(a.runners, metricsServer)
		a.closers = append(a.closers, metricsServer)
	}

	return nil
}
