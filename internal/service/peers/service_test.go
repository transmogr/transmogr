package peers

import (
	"testing"

	"github.com/transmogr/transmogr/internal/models"
)

func TestNewService_FiltersLocalAndInvalidPeers(t *testing.T) {
	t.Parallel()

	service := NewService("eu", []models.Endpoint{
		{Region: "eu", Endpoint: "eu:8443"},
		{Region: "", Endpoint: "missing-region:8443"},
		{Region: "us", Endpoint: ""},
		{Region: "us", Endpoint: "us:8443"},
		{Region: "ap", Endpoint: "ap:8443"},
	})

	peers := service.Peers()
	if len(peers) != 2 {
		t.Fatalf("len(Peers()) = %d, want 2", len(peers))
	}
	if peers[0] != (models.Peer{Region: "us", Endpoint: "us:8443"}) {
		t.Fatalf("peers[0] = %+v", peers[0])
	}
	if peers[1] != (models.Peer{Region: "ap", Endpoint: "ap:8443"}) {
		t.Fatalf("peers[1] = %+v", peers[1])
	}
}

func TestPeers_ReturnsDefensiveCopy(t *testing.T) {
	t.Parallel()

	service := NewService("eu", []models.Endpoint{
		{Region: "us", Endpoint: "us:8443"},
	})

	first := service.Peers()
	first[0].Endpoint = "mutated"

	second := service.Peers()
	if second[0].Endpoint != "us:8443" {
		t.Fatalf("Peers() returned shared slice, got endpoint %q", second[0].Endpoint)
	}
}
