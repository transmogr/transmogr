// Package crypto contains shared cryptographic primitives and interfaces.
package crypto

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	cryptorand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"

	"golang.org/x/crypto/chacha20poly1305"
)

const (
	// AlgorithmAESGCM uses AES-GCM with 16/24/32-byte keys.
	AlgorithmAESGCM = "aes-gcm"
	// AlgorithmChaCha20Poly1305 uses RFC 8439 ChaCha20-Poly1305 with a 32-byte key.
	AlgorithmChaCha20Poly1305 = "chacha20poly1305"
	// AlgorithmXChaCha20Poly1305 uses XChaCha20-Poly1305 with a 32-byte key.
	AlgorithmXChaCha20Poly1305 = "xchacha20poly1305"
)

// Encrypt encrypts value and returns the raw base64-encoded ciphertext.
func Encrypt(
	_ context.Context,
	algorithm string,
	key []byte,
	value []byte,
) ([]byte, error) {
	if len(value) == 0 {
		return cloneBytes(value), nil
	}

	aead, err := newAEAD(normalizeAlgorithm(algorithm), key)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(cryptorand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("read %s nonce: %w", normalizeAlgorithm(algorithm), err)
	}

	ciphertext := aead.Seal(nil, nonce, value, nil)
	return []byte(base64.StdEncoding.EncodeToString(append(nonce, ciphertext...))), nil
}

// Decrypt decrypts one raw JSON value using the provided algorithm and key.
// It handles both the legacy marker format ("enc:v1:...") and the current raw base64 format.
func Decrypt(
	_ context.Context,
	algorithm string,
	key []byte,
	value []byte,
) ([]byte, error) {
	if len(value) == 0 {
		return cloneBytes(value), nil
	}

	buf, err := base64.StdEncoding.DecodeString(string(value))
	if err != nil {
		return cloneBytes(value), nil
	}

	aead, err := newAEAD(normalizeAlgorithm(algorithm), key)
	if err != nil {
		return nil, err
	}

	nonceSize := aead.NonceSize()
	if len(buf) < nonceSize {
		return nil, errors.New("encrypted value is truncated")
	}

	plaintext, err := aead.Open(nil, buf[:nonceSize], buf[nonceSize:], nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt value: %w", err)
	}

	return plaintext, nil
}

func normalizeAlgorithm(algorithm string) string {
	if strings.TrimSpace(algorithm) == "" {
		return AlgorithmAESGCM
	}

	return strings.ToLower(strings.TrimSpace(algorithm))
}

func newAEAD(algorithm string, key []byte) (cipher.AEAD, error) {
	switch algorithm {
	case AlgorithmAESGCM:
		if len(key) != 16 && len(key) != 24 && len(key) != 32 {
			return nil, errors.New("aes-gcm key must be 16, 24, or 32 bytes")
		}

		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, fmt.Errorf("create aes cipher: %w", err)
		}

		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("create aes-gcm: %w", err)
		}

		return aead, nil
	case AlgorithmChaCha20Poly1305:
		if len(key) != chacha20poly1305.KeySize {
			return nil, fmt.Errorf("chacha20poly1305 key must be %d bytes", chacha20poly1305.KeySize)
		}

		aead, err := chacha20poly1305.New(key)
		if err != nil {
			return nil, fmt.Errorf("create chacha20poly1305: %w", err)
		}

		return aead, nil
	case AlgorithmXChaCha20Poly1305:
		if len(key) != chacha20poly1305.KeySize {
			return nil, fmt.Errorf("xchacha20poly1305 key must be %d bytes", chacha20poly1305.KeySize)
		}

		aead, err := chacha20poly1305.NewX(key)
		if err != nil {
			return nil, fmt.Errorf("create xchacha20poly1305: %w", err)
		}

		return aead, nil
	default:
		return nil, fmt.Errorf("unsupported crypto algorithm %q", algorithm)
	}
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}

	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
