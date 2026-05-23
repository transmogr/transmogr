package models

// Peer is a normalized outbound peer configuration.
type Peer struct {
	Region   string
	Endpoint string
}

// Endpoint is the peer definition loaded from configuration.
type Endpoint struct {
	Region   string
	Endpoint string
}
