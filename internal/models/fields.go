package models

import "encoding/json"

// CloneFields deep-copies a fields map and its byte slices.
func CloneFields(src map[string][]byte) map[string][]byte {
	if src == nil {
		return nil
	}

	dst := make(map[string][]byte, len(src))
	for key, value := range src {
		if value == nil {
			dst[key] = nil
			continue
		}

		clone := make([]byte, len(value))
		copy(clone, value)
		dst[key] = clone
	}

	return dst
}

// PayloadToFields converts a JSON object payload to a map of raw JSON field values.
func PayloadToFields(payload []byte) (map[string][]byte, error) {
	if len(payload) == 0 {
		return nil, nil
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, err
	}

	fields := make(map[string][]byte, len(raw))
	for key, value := range raw {
		fields[key] = append([]byte(nil), value...)
	}

	return fields, nil
}

// FieldsToPayload converts a fields map with raw JSON values into a JSON object payload.
func FieldsToPayload(fields map[string][]byte) ([]byte, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	raw := make(map[string]json.RawMessage, len(fields))
	for key, value := range fields {
		raw[key] = json.RawMessage(value)
	}

	return json.Marshal(raw)
}
