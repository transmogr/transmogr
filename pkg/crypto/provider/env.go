// Package provider contains crypto key providers backed by external sources.
package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	basecrypto "github.com/transmogr/transmogr/pkg/crypto"
)

// EnvConfig controls loading keys from process environment variables.
type EnvConfig struct {
	ActiveKeyID  string
	ActiveKeyEnv string
	KeyringEnv   string
}

// EnvProvider loads the active key and optional decrypt keyring from env vars.
type EnvProvider struct {
	cfg EnvConfig
}

// NewEnvProvider creates an env-backed key provider.
func NewEnvProvider(cfg EnvConfig) *EnvProvider {
	return &EnvProvider{cfg: cfg}
}

// Load reads and decodes the current env-backed key snapshot.
func (p *EnvProvider) Load(_ context.Context) (basecrypto.Snapshot, error) {
	activeEncoded := os.Getenv(p.cfg.ActiveKeyEnv)
	if activeEncoded == "" {
		return basecrypto.Snapshot{}, fmt.Errorf("%s is required", p.cfg.ActiveKeyEnv)
	}

	activeKey, err := base64.StdEncoding.DecodeString(activeEncoded)
	if err != nil {
		return basecrypto.Snapshot{}, fmt.Errorf("decode %s: %w", p.cfg.ActiveKeyEnv, err)
	}

	keysByID := map[string][]byte{
		p.cfg.ActiveKeyID: activeKey,
	}

	keyringRaw := os.Getenv(p.cfg.KeyringEnv)
	if keyringRaw != "" {
		var encodedKeys map[string]string
		if err := json.Unmarshal([]byte(keyringRaw), &encodedKeys); err != nil {
			return basecrypto.Snapshot{}, fmt.Errorf("decode %s: %w", p.cfg.KeyringEnv, err)
		}

		for keyID, encoded := range encodedKeys {
			key, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return basecrypto.Snapshot{}, fmt.Errorf("decode %s key %q: %w", p.cfg.KeyringEnv, keyID, err)
			}
			keysByID[keyID] = key
		}
	}

	return basecrypto.Snapshot{
		ActiveKeyID: p.cfg.ActiveKeyID,
		KeysByID:    keysByID,
		LoadedAt:    time.Now().UTC(),
	}, nil
}
