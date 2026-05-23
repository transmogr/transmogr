package crypto

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/transmogr/transmogr/internal/models"
	basecrypto "github.com/transmogr/transmogr/pkg/crypto"
)

func TestServiceEncryptDecryptConfiguredFields(t *testing.T) {
	t.Parallel()

	service, err := NewService(
		NewStaticKeyManager(map[string][]byte{
			"email-key":   []byte("0123456789abcdef0123456789abcdef"),
			"profile-key": []byte("abcdef0123456789abcdef0123456789"),
		}),
		[]models.TransformerSpec{
			{
				Type:       "crypto",
				Table:      "users",
				Fields:     []string{"email"},
				KeyID:      "email-key",
				CryptoType: basecrypto.AlgorithmAESGCM,
			},
			{
				Type:       "crypto",
				Table:      "users",
				Fields:     []string{"profile"},
				KeyID:      "profile-key",
				CryptoType: basecrypto.AlgorithmAESGCM,
			},
		},
	)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	fields := map[string][]byte{
		"id":      []byte(`"u-1"`),
		"email":   []byte(`"alice@example.com"`),
		"profile": []byte(`{"city":"Paris"}`),
		"age":     []byte(`30`),
	}
	encrypted, err := service.Encrypt(context.Background(), "users", fields)
	if err != nil {
		t.Fatalf("encrypt fields: %v", err)
	}
	if strings.Contains(string(encrypted["email"]), "alice@example.com") {
		t.Fatalf("expected encrypted field, got %s", encrypted["email"])
	}
	if string(encrypted["age"]) != `30` {
		t.Fatalf("expected unencrypted field to remain, got %s", encrypted["age"])
	}

	decrypted, err := service.Decrypt(context.Background(), "users", encrypted)
	if err != nil {
		t.Fatalf("decrypt fields: %v", err)
	}
	if !sameFields(decrypted, fields) {
		t.Fatalf("unexpected decrypted fields: got %#v want %#v", decrypted, fields)
	}
}

func TestServiceLeavesUnconfiguredTableUntouched(t *testing.T) {
	t.Parallel()

	service, err := NewService(NewStaticKeyManager(nil), []models.TransformerSpec{
		{
			Type:       "crypto",
			Table:      "users",
			Fields:     []string{"email"},
			KeyID:      "email-key",
			CryptoType: basecrypto.AlgorithmAESGCM,
		},
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	fields := map[string][]byte{
		"id":   []byte(`"u-1"`),
		"name": []byte(`"alice"`),
	}
	encrypted, err := service.Encrypt(context.Background(), "accounts", fields)
	if err != nil {
		t.Fatalf("encrypt fields: %v", err)
	}
	if !sameFields(encrypted, fields) {
		t.Fatalf("expected unchanged fields, got %#v", encrypted)
	}
}

func TestServiceSupportsCustomTypeHandlers(t *testing.T) {
	t.Parallel()

	service, err := NewServiceWithRegistry(
		NewStaticKeyManager(map[string][]byte{
			"custom-key": []byte("ignored"),
		}),
		[]models.TransformerSpec{
			{Type: "crypto", Table: "users", Fields: []string{"secret"}, KeyID: "custom-key", CryptoType: "reverse"},
		},
		Registry{
			"reverse": reverseHandler{},
		},
	)
	if err != nil {
		t.Fatalf("new service with registry: %v", err)
	}

	fields := map[string][]byte{
		"secret": []byte(`"abcd"`),
	}
	encrypted, err := service.Encrypt(context.Background(), "users", fields)
	if err != nil {
		t.Fatalf("encrypt fields: %v", err)
	}
	decrypted, err := service.Decrypt(context.Background(), "users", encrypted)
	if err != nil {
		t.Fatalf("decrypt fields: %v", err)
	}
	if !sameFields(decrypted, fields) {
		t.Fatalf("unexpected decrypted fields: got %#v want %#v", decrypted, fields)
	}
}

func sameFields(left, right map[string][]byte) bool {
	if len(left) != len(right) {
		return false
	}

	for key, leftValue := range left {
		rightValue, ok := right[key]
		if !ok {
			return false
		}
		if !bytes.Equal(leftValue, rightValue) {
			return false
		}
	}

	return true
}

type reverseHandler struct{}

func (reverseHandler) Encrypt(_ context.Context, _ []byte, value []byte) ([]byte, error) {
	return reverseBytes(value), nil
}

func (reverseHandler) Decrypt(_ context.Context, _ []byte, value []byte) ([]byte, error) {
	return reverseBytes(value), nil
}

func reverseBytes(src []byte) []byte {
	dst := append([]byte(nil), src...)
	for i, j := 0, len(dst)-1; i < j; i, j = i+1, j-1 {
		dst[i], dst[j] = dst[j], dst[i]
	}
	return dst
}
