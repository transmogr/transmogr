package transformers

import (
	"context"
	"fmt"
	"strings"

	"github.com/transmogr/transmogr/internal/models"
)

const (
	transitMetadataNamespace = "transit"
	cryptoMetadataNamespace  = "crypto"
)

type cryptoService interface {
	Encrypt(ctx context.Context, table string, fields map[string][]byte) (map[string][]byte, error)
	Decrypt(ctx context.Context, table string, fields map[string][]byte) (map[string][]byte, error)
	EncryptedFieldNames(table string) []string
}

// CryptoOutboundTransformer decrypts local ciphertext after reading from the
// outbox and before the payload is serialized onto the wire.
type CryptoOutboundTransformer struct {
	crypto cryptoService
}

// NewCryptoOutboundTransformer creates a crypto-aware outbound transformer.
func NewCryptoOutboundTransformer(crypto cryptoService) CryptoOutboundTransformer {
	return CryptoOutboundTransformer{crypto: crypto}
}

// PrepareOutboundBatch decrypts encrypted fields, moves them into transit
// metadata, and removes them from the outbound wire payload.
func (p CryptoOutboundTransformer) PrepareOutboundBatch(
	ctx context.Context,
	batch models.OutboxBatch,
) (models.OutboxBatch, error) {
	if p.crypto == nil {
		return batch, nil
	}

	for i, event := range batch.Events {
		fields, err := p.crypto.Decrypt(ctx, event.Table, event.Fields)
		if err != nil {
			return models.OutboxBatch{}, fmt.Errorf("prepare outbound batch event %q: %w", event.EventID, err)
		}
		batch.Events[i].Fields = fields
		if batch.Events[i].Metadata == nil {
			batch.Events[i].Metadata = make(map[string][]byte)
		}
		for _, field := range p.crypto.EncryptedFieldNames(event.Table) {
			value, ok := batch.Events[i].Fields[field]
			if !ok {
				continue
			}
			batch.Events[i].Metadata[TransitKey(field)] = value
			delete(batch.Events[i].Fields, field)
		}
	}

	return batch, nil
}

// PrepareOutboundSnapshotRows applies the same outbound crypto transform to snapshot rows.
func (p CryptoOutboundTransformer) PrepareOutboundSnapshotRows(
	ctx context.Context,
	table string,
	rows []models.SnapshotRow,
) ([]models.SnapshotRow, error) {
	if p.crypto == nil {
		return rows, nil
	}

	for i := range rows {
		fields, err := models.PayloadToFields(rows[i].Payload)
		if err != nil {
			return nil, fmt.Errorf("decode snapshot row payload: %w", err)
		}
		fields, err = p.crypto.Decrypt(ctx, table, fields)
		if err != nil {
			return nil, fmt.Errorf("prepare outbound snapshot row: %w", err)
		}
		if rows[i].Metadata == nil {
			rows[i].Metadata = make(map[string][]byte)
		}
		for _, field := range p.crypto.EncryptedFieldNames(table) {
			value, ok := fields[field]
			if !ok {
				continue
			}
			rows[i].Metadata[TransitKey(field)] = value
			delete(fields, field)
		}
		payload, err := models.FieldsToPayload(fields)
		if err != nil {
			return nil, fmt.Errorf("encode outbound snapshot row payload: %w", err)
		}
		rows[i].Payload = payload
	}

	return rows, nil
}

// CryptoInboundTransformer encrypts received plaintext before local apply.
type CryptoInboundTransformer struct {
	crypto cryptoService
}

// NewCryptoInboundTransformer creates a crypto-aware inbound transformer.
func NewCryptoInboundTransformer(crypto cryptoService) CryptoInboundTransformer {
	return CryptoInboundTransformer{crypto: crypto}
}

// PrepareInboundEvents restores encrypted fields from transit metadata and
// re-encrypts them for local storage.
func (p CryptoInboundTransformer) PrepareInboundEvents(
	ctx context.Context,
	events []models.ReplicationEvent,
) ([]models.ReplicationEvent, error) {
	if p.crypto == nil {
		return events, nil
	}

	for i, event := range events {
		if event.Operation == models.ReplicationOperationDelete {
			continue
		}

		if events[i].Fields == nil {
			events[i].Fields = make(map[string][]byte)
		}
		for key, value := range events[i].Metadata {
			field, ok := ParseTransitKey(key)
			if !ok {
				continue
			}
			events[i].Fields[field] = value
			delete(events[i].Metadata, key)
		}

		fields, err := p.crypto.Encrypt(ctx, events[i].Table, events[i].Fields)
		if err != nil {
			return nil, fmt.Errorf("prepare inbound event %q: %w", events[i].EventID, err)
		}
		events[i].Fields = fields
	}

	return events, nil
}

// PrepareInboundSnapshotRows restores encrypted snapshot fields from transit
// metadata and re-encrypts them for local storage.
func (p CryptoInboundTransformer) PrepareInboundSnapshotRows(
	ctx context.Context,
	table string,
	rows []models.SnapshotRow,
) ([]models.SnapshotRow, error) {
	if p.crypto == nil {
		return rows, nil
	}

	for i := range rows {
		fields, err := models.PayloadToFields(rows[i].Payload)
		if err != nil {
			return nil, fmt.Errorf("decode inbound snapshot row payload: %w", err)
		}
		for key, value := range rows[i].Metadata {
			field, ok := ParseTransitKey(key)
			if !ok {
				continue
			}
			if fields == nil {
				fields = make(map[string][]byte)
			}
			fields[field] = value
			delete(rows[i].Metadata, key)
		}
		fields, err = p.crypto.Encrypt(ctx, table, fields)
		if err != nil {
			return nil, fmt.Errorf("prepare inbound snapshot row: %w", err)
		}
		payload, err := models.FieldsToPayload(fields)
		if err != nil {
			return nil, fmt.Errorf("encode inbound snapshot row payload: %w", err)
		}
		rows[i].Payload = payload
	}

	return rows, nil
}

// TransitKey builds the transit metadata key used for crypto-owned fields.
func TransitKey(field string) string {
	if field == "" {
		return ""
	}
	return transitMetadataNamespace + "." + cryptoMetadataNamespace + "." + field
}

// ParseTransitKey extracts the field name from a crypto-owned transit metadata key.
func ParseTransitKey(key string) (string, bool) {
	const prefix = transitMetadataNamespace + "." + cryptoMetadataNamespace + "."
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	field := strings.TrimPrefix(key, prefix)
	if !isValidCryptoTransitField(field) {
		return "", false
	}
	return field, true
}

func isValidCryptoTransitField(field string) bool {
	if field == "" {
		return false
	}
	for _, r := range field {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}
