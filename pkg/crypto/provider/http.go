package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	basecrypto "github.com/transmogr/transmogr/pkg/crypto"
)

// HTTPConfig controls loading keys from an HTTP endpoint.
type HTTPConfig struct {
	URL        string
	Timeout    time.Duration
	TokenEnv   string
	HeaderName string
}

// HTTPProvider loads a full key snapshot from an HTTP endpoint.
type HTTPProvider struct {
	cfg    HTTPConfig
	client *http.Client
}

type httpResponse struct {
	ActiveKeyID string            `json:"active_key_id"`
	Keys        map[string]string `json:"keys"`
	Version     string            `json:"version"`
}

// NewHTTPProvider creates an HTTP-backed key provider.
func NewHTTPProvider(cfg HTTPConfig) *HTTPProvider {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.HeaderName == "" {
		cfg.HeaderName = "Authorization"
	}

	return &HTTPProvider{
		cfg: cfg,
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
	}
}

// Load fetches and decodes the current HTTP-backed key snapshot.
func (p *HTTPProvider) Load(ctx context.Context) (basecrypto.Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.URL, nil)
	if err != nil {
		return basecrypto.Snapshot{}, fmt.Errorf("build key request: %w", err)
	}

	if p.cfg.TokenEnv != "" {
		if token := os.Getenv(p.cfg.TokenEnv); token != "" {
			req.Header.Set(p.cfg.HeaderName, token)
		}
	}

	resp, err := p.client.Do(req) //nolint:gosec
	if err != nil {
		return basecrypto.Snapshot{}, fmt.Errorf("fetch keys: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return basecrypto.Snapshot{}, fmt.Errorf("fetch keys: unexpected status %d", resp.StatusCode)
	}

	const maxResponseBytes = 1 << 20 // 1 MiB
	var payload httpResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return basecrypto.Snapshot{}, fmt.Errorf("decode key response: %w", err)
	}

	keysByID := make(map[string][]byte, len(payload.Keys))
	for keyID, encoded := range payload.Keys {
		key, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return basecrypto.Snapshot{}, fmt.Errorf("decode http key %q: %w", keyID, err)
		}
		keysByID[keyID] = key
	}

	return basecrypto.Snapshot{
		ActiveKeyID: payload.ActiveKeyID,
		KeysByID:    keysByID,
		LoadedAt:    time.Now().UTC(),
		Version:     payload.Version,
	}, nil
}
