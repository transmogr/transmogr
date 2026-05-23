package models

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// PrimaryKeyType identifies the normalized value type of one PK part.
type PrimaryKeyType string

// Primary key type constants for known PostgreSQL column types.
const (
	PrimaryKeyTypeUnspecified PrimaryKeyType = ""
	PrimaryKeyTypeString      PrimaryKeyType = "string"
	PrimaryKeyTypeInteger     PrimaryKeyType = "integer"
	PrimaryKeyTypeBoolean     PrimaryKeyType = "boolean"
)

// PrimaryKeyPart is one column/value pair from a primary key.
type PrimaryKeyPart struct {
	Column string          `json:"column"`
	Type   PrimaryKeyType  `json:"type"`
	Value  json.RawMessage `json:"json_value"`
}

// EncodePrimaryKey serializes a primary key into a stable wire/state format.
func EncodePrimaryKey(parts []PrimaryKeyPart) ([]byte, error) {
	if len(parts) == 0 {
		return nil, nil
	}

	encoded, err := json.Marshal(parts)
	if err != nil {
		return nil, fmt.Errorf("encode primary key: %w", err)
	}

	return encoded, nil
}

// MustEncodePrimaryKey serializes a primary key and panics on error.
func MustEncodePrimaryKey(parts []PrimaryKeyPart) []byte {
	encoded, err := EncodePrimaryKey(parts)
	if err != nil {
		panic(err)
	}

	return encoded
}

// DecodePrimaryKey parses a serialized primary key.
func DecodePrimaryKey(data []byte) ([]PrimaryKeyPart, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var parts []PrimaryKeyPart
	if err := json.Unmarshal(data, &parts); err != nil {
		return nil, fmt.Errorf("decode primary key: %w", err)
	}

	return parts, nil
}

// ClonePrimaryKeyParts deep-copies primary key parts.
func ClonePrimaryKeyParts(src []PrimaryKeyPart) []PrimaryKeyPart {
	if src == nil {
		return nil
	}

	dst := make([]PrimaryKeyPart, len(src))
	for i, part := range src {
		dst[i] = PrimaryKeyPart{
			Column: part.Column,
			Type:   part.Type,
			Value:  append(json.RawMessage(nil), part.Value...),
		}
	}

	return dst
}

// ComparePrimaryKeys provides a stable ordering for serialized primary keys.
func ComparePrimaryKeys(left, right []PrimaryKeyPart) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}

	for i := 0; i < limit; i++ {
		if cmp := comparePrimaryKeyPart(left[i], right[i]); cmp != 0 {
			return cmp
		}
	}

	switch {
	case len(left) < len(right):
		return -1
	case len(left) > len(right):
		return 1
	default:
		return 0
	}
}

// PrimaryKeyPartsFromPayload extracts primary key parts from a row payload.
func PrimaryKeyPartsFromPayload(
	payload []byte, columns []string, types map[string]PrimaryKeyType,
) ([]PrimaryKeyPart, error) {
	var data map[string]json.RawMessage
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, fmt.Errorf("decode primary key payload: %w", err)
	}

	return PrimaryKeyPartsFromMap(data, columns, types)
}

// PrimaryKeyPartsFromMap builds primary key parts from a JSON object map.
func PrimaryKeyPartsFromMap(
	data map[string]json.RawMessage, columns []string, types map[string]PrimaryKeyType,
) ([]PrimaryKeyPart, error) {
	parts := make([]PrimaryKeyPart, 0, len(columns))
	for _, column := range columns {
		value, ok := data[column]
		if !ok {
			return nil, fmt.Errorf("missing primary key column %q", column)
		}
		parts = append(parts, PrimaryKeyPart{
			Column: column,
			Type:   types[column],
			Value:  append(json.RawMessage(nil), value...),
		})
	}

	return parts, nil
}

// PrimaryKeyMap returns the primary key as a JSON object keyed by column name.
func PrimaryKeyMap(parts []PrimaryKeyPart) map[string]json.RawMessage {
	if len(parts) == 0 {
		return nil
	}

	result := make(map[string]json.RawMessage, len(parts))
	for _, part := range parts {
		result[part.Column] = append(json.RawMessage(nil), part.Value...)
	}

	return result
}

// PrimaryKeyString builds a single-column primary key from a string value.
func PrimaryKeyString(column, value string) []PrimaryKeyPart {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}

	return []PrimaryKeyPart{{
		Column: column,
		Type:   PrimaryKeyTypeString,
		Value:  raw,
	}}
}

// PrimaryKeyValueFromString converts a textual value into JSON matching the normalized PK type.
func PrimaryKeyValueFromString(typ PrimaryKeyType, value string) (json.RawMessage, error) {
	switch typ {
	case PrimaryKeyTypeInteger:
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return nil, fmt.Errorf("parse %s primary key value %q: %w", typ, value, err)
		}
		return json.RawMessage(value), nil
	case PrimaryKeyTypeBoolean:
		if _, err := strconv.ParseBool(value); err != nil {
			return nil, fmt.Errorf("parse bool primary key value %q: %w", value, err)
		}
		return json.RawMessage(value), nil
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("marshal primary key value for %s: %w", typ, err)
		}
		return raw, nil
	}
}

func comparePrimaryKeyPart(left, right PrimaryKeyPart) int {
	if left.Column < right.Column {
		return -1
	}
	if left.Column > right.Column {
		return 1
	}

	if left.Type < right.Type {
		return -1
	}
	if left.Type > right.Type {
		return 1
	}

	return comparePrimaryKeyValue(left.Type, left.Value, right.Value)
}

func comparePrimaryKeyValue(typ PrimaryKeyType, left, right json.RawMessage) int {
	switch typ {
	case PrimaryKeyTypeInteger:
		return compareIntegerJSON(left, right)
	case PrimaryKeyTypeBoolean:
		return compareBoolJSON(left, right)
	default:
		return bytes.Compare(left, right)
	}
}

func compareIntegerJSON(left, right json.RawMessage) int {
	var l, r int64
	if err := json.Unmarshal(left, &l); err != nil {
		return bytes.Compare(left, right)
	}
	if err := json.Unmarshal(right, &r); err != nil {
		return bytes.Compare(left, right)
	}

	switch {
	case l < r:
		return -1
	case l > r:
		return 1
	default:
		return 0
	}
}

func compareBoolJSON(left, right json.RawMessage) int {
	var l, r bool
	if err := json.Unmarshal(left, &l); err != nil {
		return bytes.Compare(left, right)
	}
	if err := json.Unmarshal(right, &r); err != nil {
		return bytes.Compare(left, right)
	}

	switch {
	case l == r:
		return 0
	case !l && r:
		return -1
	default:
		return 1
	}
}
