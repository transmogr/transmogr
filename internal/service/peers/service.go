// Package peers manages the configured set of remote replication peers.
package peers

import "github.com/transmogr/transmogr/internal/models"

// Service stores the set of peers eligible for outbound connections.
type Service struct {
	localRegion string
	peers       []models.Peer
}

// NewService builds a peer service from configured endpoints.
func NewService(localRegion string, endpoints []models.Endpoint) *Service {
	peers := make([]models.Peer, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.Region == "" || endpoint.Endpoint == "" || endpoint.Region == localRegion {
			continue
		}

		peers = append(peers, models.Peer(endpoint))
	}

	return &Service{
		localRegion: localRegion,
		peers:       peers,
	}
}

// Peers returns a defensive copy of configured peers.
func (s *Service) Peers() []models.Peer {
	result := make([]models.Peer, len(s.peers))
	copy(result, s.peers)
	return result
}
