// Package client manages outbound replication gRPC client connections.
package client

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"time"
)

const maxReconnectDelay = 30 * time.Second

type backoff struct {
	baseDelay time.Duration
	maxDelay  time.Duration
	randomInt func(int64) int64
}

func newBackoff(baseDelay time.Duration) *backoff {
	return &backoff{
		baseDelay: baseDelay,
		maxDelay:  maxReconnectDelay,
		randomInt: cryptoRandomInt,
	}
}

func (b *backoff) duration(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	delay := b.baseDelay
	for i := 0; i < attempt; i++ {
		if delay >= b.maxDelay {
			delay = b.maxDelay
			break
		}

		delay *= 2
		if delay > b.maxDelay {
			delay = b.maxDelay
		}
	}

	// Add bounded jitter so peers do not reconnect in lockstep.
	if delay <= 0 {
		return delay
	}

	jitter := time.Duration(b.randomInt(int64(delay)))
	delay = (delay / 2) + jitter
	if delay < b.baseDelay {
		return b.baseDelay
	}

	return delay
}

func cryptoRandomInt(limit int64) int64 {
	if limit <= 1 {
		return 0
	}

	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return 0
	}

	value := binary.BigEndian.Uint64(bytes[:]) % uint64(limit)
	if value > math.MaxInt64 {
		return 0
	}

	return int64(value)
}
