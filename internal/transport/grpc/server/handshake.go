package server

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/transmogr/transmogr/internal/models"
	grpcprotocol "github.com/transmogr/transmogr/internal/transport/grpc/protocol"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

// handleConsumeRequest processes the initial consume request from the remote peer.
// It validates the remote configuration and returns the verified consumer region.
func (h *Handler) handleConsumeRequest(
	ctx context.Context,
	req *peerv1.ConsumeRequest,
	localHandshake models.HandshakeMessage,
) (remoteRegion string, err error) {
	hs := &peerv1.Handshake{
		Region:        req.GetRegion(),
		Version:       req.GetVersion(),
		Configuration: req.GetConfiguration(),
	}
	if err := grpcprotocol.CheckHandshakeCompatibility(localHandshake, hs); err != nil {
		return "", status.Errorf(codes.FailedPrecondition, "%v", err)
	}

	remoteRegion = hs.GetRegion()
	if err := h.validateRemoteRegion(ctx, remoteRegion); err != nil {
		return "", err
	}

	return remoteRegion, nil
}

// validateRemoteRegion checks that the remote region is acceptable: non-empty,
// not equal to the local region, and present in the allowed-peer set (if configured).
// It also verifies the region matches the TLS certificate when mTLS is in use.
func (h *Handler) validateRemoteRegion(ctx context.Context, remoteRegion string) error {
	if remoteRegion == "" {
		return status.Error(codes.PermissionDenied, "peer region is required")
	}
	if h.localRegion != "" && remoteRegion == h.localRegion {
		return status.Error(codes.PermissionDenied, "peer region must not equal local region")
	}
	if len(h.allowedPeerRegions) > 0 {
		if _, ok := h.allowedPeerRegions[remoteRegion]; !ok {
			return status.Errorf(codes.PermissionDenied, "peer region %q is not allowed", remoteRegion)
		}
	}

	certRegion, err := certRegionFromContext(ctx)
	if err != nil {
		return err
	}
	if certRegion != "" && certRegion != remoteRegion {
		return status.Errorf(
			codes.PermissionDenied,
			"handshake region %q does not match certificate region %q",
			remoteRegion, certRegion,
		)
	}

	return nil
}

// certRegionFromContext extracts the peer region from the URI SAN of the client's
// TLS certificate. The expected URI format is transmogr://region/<region-name>.
// Returns ("", nil) when the connection is not using TLS — the region check is skipped.
// Returns an error when TLS is in use but a valid region URI SAN cannot be found.
func certRegionFromContext(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return "", nil
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", nil
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return "", status.Error(codes.Unauthenticated, "no client certificate")
	}
	for _, uri := range tlsInfo.State.PeerCertificates[0].URIs {
		if uri.Scheme == "transmogr" && uri.Host == "region" {
			if r := strings.TrimPrefix(uri.Path, "/"); r != "" {
				return r, nil
			}
		}
	}
	return "", status.Error(codes.Unauthenticated, "client certificate missing region URI SAN (transmogr://region/<name>)")
}
