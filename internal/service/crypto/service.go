// Package crypto provides table-aware encryption policy and field transforms.
package crypto

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/transmogr/transmogr/internal/models"
	basecrypto "github.com/transmogr/transmogr/pkg/crypto"
)

// KeyManager returns crypto keys by key id.
type KeyManager interface {
	GetKey(ctx context.Context, keyID string) ([]byte, error)
}

// TypeHandler encrypts and decrypts one configured crypto type.
type TypeHandler interface {
	Encrypt(ctx context.Context, key []byte, value []byte) ([]byte, error)
	Decrypt(ctx context.Context, key []byte, value []byte) ([]byte, error)
}

type fieldCryptoConfig struct {
	typeName string
	keyID    string
	handler  TypeHandler
}

// Service applies table crypto policy over configured values and key lookup.
type Service struct {
	keyManager    KeyManager
	cryptoByTable map[string]map[string]fieldCryptoConfig
}

// Registry resolves configured crypto types to handlers.
type Registry map[string]TypeHandler

type staticKeyManager struct {
	keysByID map[string][]byte
}

// NewStaticKeyManager returns an in-memory key manager.
func NewStaticKeyManager(keysByID map[string][]byte) KeyManager {
	cloned := make(map[string][]byte, len(keysByID))
	for keyID, key := range keysByID {
		cloned[keyID] = append([]byte(nil), key...)
	}

	return staticKeyManager{keysByID: cloned}
}

// NewService creates a table-aware crypto service over key lookup and low-level value crypto.
func NewService(
	keyManager KeyManager,
	transformers []models.TransformerSpec,
) (*Service, error) {
	return NewServiceWithRegistry(keyManager, transformers, DefaultRegistry())
}

// NewServiceWithRegistry creates a table-aware crypto service over an explicit handler registry.
func NewServiceWithRegistry(
	keyManager KeyManager,
	transformers []models.TransformerSpec,
	registry Registry,
) (*Service, error) {
	cryptoByTable, err := encryptedFieldsByTable(transformers, registry)
	if err != nil {
		return nil, err
	}
	return &Service{
		keyManager:    keyManager,
		cryptoByTable: cryptoByTable,
	}, nil
}

// Encrypt encrypts configured fields in a table field set for local storage.
func (s *Service) Encrypt(
	ctx context.Context,
	table string,
	fields map[string][]byte,
) (map[string][]byte, error) {
	cfg, ok := s.cryptoByTable[table]
	if !ok || len(fields) == 0 {
		return models.CloneFields(fields), nil
	}

	result := models.CloneFields(fields)
	keyCache := make(map[string][]byte)
	for field, fieldCfg := range cfg {
		raw, ok := result[field]
		if !ok {
			continue
		}

		key, ok := keyCache[fieldCfg.keyID]
		var err error
		if !ok {
			key, err = s.keyManager.GetKey(ctx, fieldCfg.keyID)
			if err != nil {
				return nil, fmt.Errorf("load crypto key %q for %s: %w", fieldCfg.keyID, table, err)
			}
			keyCache[fieldCfg.keyID] = key
		}

		ciphertext, err := fieldCfg.handler.Encrypt(ctx, key, raw)
		if err != nil {
			return nil, fmt.Errorf("encrypt field %q for %s using %s: %w", field, table, fieldCfg.typeName, err)
		}
		jsonValue, err := json.Marshal(string(ciphertext))
		if err != nil {
			return nil, fmt.Errorf("encode encrypted field %q for %s: %w", field, table, err)
		}
		result[field] = jsonValue
	}

	return result, nil
}

// Decrypt decrypts configured fields in a table field set for outbound replication.
func (s *Service) Decrypt(
	ctx context.Context,
	table string,
	fields map[string][]byte,
) (map[string][]byte, error) {
	cfg, ok := s.cryptoByTable[table]
	if !ok || len(fields) == 0 {
		return models.CloneFields(fields), nil
	}

	result := models.CloneFields(fields)
	keyCache := make(map[string][]byte)
	for field, fieldCfg := range cfg {
		raw, ok := result[field]
		if !ok {
			continue
		}

		// Field values are JSON strings holding the base64 ciphertext.
		// PostgreSQL to_jsonb() encodes BYTEA as "\xHEX..." in JSON strings —
		// strip the prefix and hex-decode to recover the actual base64 ciphertext bytes.
		var encoded string
		if err := json.Unmarshal(raw, &encoded); err != nil {
			continue // not a JSON string — field is not encrypted, leave as-is
		}

		ciphertext := []byte(encoded)
		if strings.HasPrefix(encoded, `\x`) {
			if decoded, err := hex.DecodeString(encoded[2:]); err == nil {
				ciphertext = decoded
			}
		}

		key, ok := keyCache[fieldCfg.keyID]
		var err error
		if !ok {
			key, err = s.keyManager.GetKey(ctx, fieldCfg.keyID)
			if err != nil {
				return nil, fmt.Errorf("load crypto key %q for %s: %w", fieldCfg.keyID, table, err)
			}
			keyCache[fieldCfg.keyID] = key
		}

		plaintext, err := fieldCfg.handler.Decrypt(ctx, key, ciphertext)
		if err != nil {
			return nil, fmt.Errorf("decrypt field %q for %s using %s: %w", field, table, fieldCfg.typeName, err)
		}
		result[field] = plaintext
	}

	return result, nil
}

// EncryptedFieldNames returns the configured encrypted field names for one table.
func (s *Service) EncryptedFieldNames(table string) []string {
	cfg, ok := s.cryptoByTable[table]
	if !ok || len(cfg) == 0 {
		return nil
	}

	fields := make([]string, 0, len(cfg))
	for field := range cfg {
		fields = append(fields, field)
	}
	slices.Sort(fields)
	return fields
}

func (m staticKeyManager) GetKey(_ context.Context, keyID string) ([]byte, error) {
	key, ok := m.keysByID[keyID]
	if !ok {
		return nil, fmt.Errorf("crypto key %q is not configured", keyID)
	}

	return append([]byte(nil), key...), nil
}

func encryptedFieldsByTable(
	transformers []models.TransformerSpec,
	registry Registry,
) (map[string]map[string]fieldCryptoConfig, error) {
	fieldsByTable := make(map[string]map[string]fieldCryptoConfig)
	for _, spec := range transformers {
		if !strings.EqualFold(strings.TrimSpace(spec.Type), "crypto") {
			continue
		}
		cryptoType := strings.ToLower(strings.TrimSpace(spec.CryptoType))
		handler, ok := registry[cryptoType]
		if !ok {
			return nil, fmt.Errorf(
				"crypto transformer for table %q references unsupported crypto type %q",
				spec.Table,
				spec.CryptoType,
			)
		}
		tableName := strings.TrimSpace(spec.Table)
		fields := fieldsByTable[tableName]
		if fields == nil {
			fields = make(map[string]fieldCryptoConfig)
			fieldsByTable[tableName] = fields
		}
		for _, field := range spec.Fields {
			if field == "" {
				continue
			}
			fields[field] = fieldCryptoConfig{
				typeName: spec.CryptoType,
				keyID:    spec.KeyID,
				handler:  handler,
			}
		}
	}

	return fieldsByTable, nil
}

// DefaultRegistry returns the built-in crypto type handlers.
func DefaultRegistry() Registry {
	return Registry{
		basecrypto.AlgorithmAESGCM:            AlgorithmHandler(basecrypto.AlgorithmAESGCM),
		basecrypto.AlgorithmChaCha20Poly1305:  AlgorithmHandler(basecrypto.AlgorithmChaCha20Poly1305),
		basecrypto.AlgorithmXChaCha20Poly1305: AlgorithmHandler(basecrypto.AlgorithmXChaCha20Poly1305),
	}
}

// AlgorithmHandler adapts a pkg/crypto algorithm name into a TypeHandler.
func AlgorithmHandler(algorithm string) TypeHandler {
	return algorithmHandler{algorithm: algorithm}
}

type algorithmHandler struct {
	algorithm string
}

func (h algorithmHandler) Encrypt(ctx context.Context, key []byte, value []byte) ([]byte, error) {
	return basecrypto.Encrypt(ctx, h.algorithm, key, value)
}

func (h algorithmHandler) Decrypt(ctx context.Context, key []byte, value []byte) ([]byte, error) {
	return basecrypto.Decrypt(ctx, h.algorithm, key, value)
}
