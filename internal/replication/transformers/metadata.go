package transformers

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/transmogr/transmogr/internal/models"
)

const (
	metadataNamespaceTransit = "transit"
)

// IsTransitMetadataKey reports whether the metadata key is transit-only.
func IsTransitMetadataKey(key string) bool {
	return key == metadataNamespaceTransit || strings.HasPrefix(key, metadataNamespaceTransit+".")
}

// IsValidMetadataKey reports whether a metadata key follows the shared dotted-namespace format.
func IsValidMetadataKey(key string) bool {
	if key == "" || strings.HasPrefix(key, ".") || strings.HasSuffix(key, ".") || strings.Contains(key, "..") {
		return false
	}

	for _, part := range strings.Split(key, ".") {
		if !isValidMetadataSegment(part) {
			return false
		}
	}

	return true
}

// InboundMetadataTransformer validates metadata keys and removes any remaining transit-only entries.
type InboundMetadataTransformer struct{}

// NewInboundMetadataTransformer returns an inbound transformer that validates
// and removes transit-only metadata before local apply.
func NewInboundMetadataTransformer() InboundMetadataTransformer {
	return InboundMetadataTransformer{}
}

// PrepareInboundEvents validates event metadata keys and drops transit-only
// metadata before the events reach storage.
func (p InboundMetadataTransformer) PrepareInboundEvents(
	_ context.Context,
	events []models.ReplicationEvent,
) ([]models.ReplicationEvent, error) {
	for i := range events {
		for key := range events[i].Metadata {
			if !IsValidMetadataKey(key) {
				return nil, fmt.Errorf("event %q has invalid metadata key %q", events[i].EventID, key)
			}
			if IsTransitMetadataKey(key) {
				delete(events[i].Metadata, key)
			}
		}
	}

	return events, nil
}

// PrepareInboundSnapshotRows validates snapshot row metadata keys and removes
// transit-only metadata before local apply.
func (p InboundMetadataTransformer) PrepareInboundSnapshotRows(
	_ context.Context,
	_ string,
	rows []models.SnapshotRow,
) ([]models.SnapshotRow, error) {
	for i := range rows {
		for key := range rows[i].Metadata {
			if !IsValidMetadataKey(key) {
				return nil, fmt.Errorf("snapshot row has invalid metadata key %q", key)
			}
			if IsTransitMetadataKey(key) {
				delete(rows[i].Metadata, key)
			}
		}
	}

	return rows, nil
}

func isValidMetadataSegment(part string) bool {
	if part == "" {
		return false
	}

	for _, r := range part {
		if unicode.IsLower(r) || unicode.IsDigit(r) || r == '_' || r == '-' {
			continue
		}
		return false
	}

	return true
}
