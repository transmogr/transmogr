package app

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/transmogr/transmogr/internal/models"
	cryptoservice "github.com/transmogr/transmogr/internal/service/crypto"
	basecrypto "github.com/transmogr/transmogr/pkg/crypto"
	"github.com/transmogr/transmogr/pkg/crypto/provider"
)

// isConfigured reports whether s contains a non-blank value.
func isConfigured(s string) bool {
	return strings.TrimSpace(s) != ""
}

func newCryptoKeyManager(ctx context.Context, cfg Config) (cryptoservice.KeyManager, Runner, error) {
	if !hasEncryptedTransformers(cfg.Transformers) {
		return cryptoservice.NewStaticKeyManager(nil), nil, nil
	}

	p, err := newCryptoProvider(cfg)
	if err != nil {
		return nil, nil, err
	}

	manager, err := basecrypto.NewCachedManager(ctx, p, basecrypto.KeyManagerConfig{
		RefreshInterval: cfg.Crypto.RefreshInterval,
		RetryAttempts:   cfg.Crypto.RetryAttempts,
		RetryDelay:      cfg.Crypto.RetryDelay,
		RequiredKeyIDs:  requiredCryptoKeyIDs(cfg.Transformers),
	})
	if err != nil {
		return nil, nil, err
	}

	return manager, manager, nil
}

func newCryptoProvider(cfg Config) (basecrypto.Provider, error) {
	providers := make([]basecrypto.NamedProvider, 0, 3)
	if isConfigured(cfg.Crypto.Source.HTTP.URL) {
		providers = append(providers, basecrypto.NamedProvider{
			Name: "http",
			Provider: provider.NewHTTPProvider(provider.HTTPConfig{
				URL:        cfg.Crypto.Source.HTTP.URL,
				Timeout:    cfg.Crypto.Source.HTTP.Timeout,
				TokenEnv:   cfg.Crypto.Source.HTTP.TokenEnv,
				HeaderName: cfg.Crypto.Source.HTTP.HeaderName,
			}),
		})
	}
	if isConfigured(cfg.Crypto.Source.Vault.Address) && isConfigured(cfg.Crypto.Source.Vault.Path) {
		providers = append(providers, basecrypto.NamedProvider{
			Name: "vault",
			Provider: provider.NewVaultProvider(provider.VaultConfig{
				Address:  cfg.Crypto.Source.Vault.Address,
				TokenEnv: cfg.Crypto.Source.Vault.TokenEnv,
				Path:     cfg.Crypto.Source.Vault.Path,
				Timeout:  cfg.Crypto.Source.Vault.Timeout,
			}),
		})
	}
	if os.Getenv(cfg.Crypto.Source.Env.ActiveKeyEnv) != "" || os.Getenv(cfg.Crypto.Source.Env.KeyringEnv) != "" {
		providers = append(providers, basecrypto.NamedProvider{
			Name: "env",
			Provider: provider.NewEnvProvider(provider.EnvConfig{
				ActiveKeyID:  cfg.Crypto.Source.Env.ActiveKeyID,
				ActiveKeyEnv: cfg.Crypto.Source.Env.ActiveKeyEnv,
				KeyringEnv:   cfg.Crypto.Source.Env.KeyringEnv,
			}),
		})
	}

	if len(providers) == 0 {
		return nil, fmt.Errorf("no crypto key sources configured or available")
	}

	return &basecrypto.MergeProvider{Providers: providers}, nil
}

func hasEncryptedTransformers(transformers []models.TransformerSpec) bool {
	for _, transformer := range transformers {
		if strings.EqualFold(strings.TrimSpace(transformer.Type), "crypto") {
			return true
		}
	}

	return false
}

func requiredCryptoKeyIDs(transformers []models.TransformerSpec) []string {
	keyIDs := make(map[string]struct{})
	for _, transformer := range transformers {
		if !strings.EqualFold(strings.TrimSpace(transformer.Type), "crypto") {
			continue
		}
		if !isConfigured(transformer.KeyID) {
			continue
		}
		keyIDs[transformer.KeyID] = struct{}{}
	}

	result := make([]string, 0, len(keyIDs))
	for keyID := range keyIDs {
		result = append(result, keyID)
	}

	return result
}

func (a *App) initCrypto(ctx context.Context) (cryptoservice.KeyManager, error) {
	km, cryptoKeyRunner, err := newCryptoKeyManager(ctx, a.cfg)
	if err != nil {
		return nil, err
	}

	if cryptoKeyRunner != nil {
		a.runners = append(a.runners, cryptoKeyRunner)
	}
	return km, nil
}
