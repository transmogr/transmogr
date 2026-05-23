package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	basecrypto "github.com/transmogr/transmogr/pkg/crypto"
)

// VaultConfig controls loading keys from the Vault HTTP API.
type VaultConfig struct {
	Address  string
	TokenEnv string
	Path     string
	Timeout  time.Duration
}

// VaultProvider loads a full key snapshot from a Vault KV v2 secret.
type VaultProvider struct {
	cfg    VaultConfig
	client *http.Client
}

type vaultResponse struct {
	Data struct {
		Data struct {
			ActiveKeyID string            `json:"active_key_id"`
			Keys        map[string]string `json:"keys"`
			Version     string            `json:"version"`
		} `json:"data"`
	} `json:"data"`
}

// NewVaultProvider creates a Vault-backed key provider.
func NewVaultProvider(cfg VaultConfig) *VaultProvider {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}

	return &VaultProvider{
		cfg: cfg,
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
	}
}

// Load fetches and decodes the current Vault-backed key snapshot.
func (p *VaultProvider) Load(ctx context.Context) (basecrypto.Snapshot, error) {
	path := strings.TrimLeft(strings.TrimSpace(p.cfg.Path), "/")
	url := strings.TrimRight(strings.TrimSpace(p.cfg.Address), "/") + "/v1/" + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return basecrypto.Snapshot{}, fmt.Errorf("build vault key request: %w", err)
	}

	if p.cfg.TokenEnv != "" {
		if token := os.Getenv(p.cfg.TokenEnv); token != "" {
			req.Header.Set("X-Vault-Token", token)
		}
	}

	resp, err := p.client.Do(req) //nolint:gosec
	if err != nil {
		return basecrypto.Snapshot{}, fmt.Errorf("fetch vault keys: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return basecrypto.Snapshot{}, fmt.Errorf("fetch vault keys: unexpected status %d", resp.StatusCode)
	}

	var payload vaultResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return basecrypto.Snapshot{}, fmt.Errorf("decode vault key response: %w", err)
	}

	keysByID := make(map[string][]byte, len(payload.Data.Data.Keys))
	for keyID, encoded := range payload.Data.Data.Keys {
		key, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return basecrypto.Snapshot{}, fmt.Errorf("decode vault key %q: %w", keyID, err)
		}
		keysByID[keyID] = key
	}

	return basecrypto.Snapshot{
		ActiveKeyID: payload.Data.Data.ActiveKeyID,
		KeysByID:    keysByID,
		LoadedAt:    time.Now().UTC(),
		Version:     payload.Data.Data.Version,
	}, nil
}
