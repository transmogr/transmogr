package client

import (
	"testing"
	"time"
)

func TestBackoffDurationGrowsAndCaps(t *testing.T) {
	t.Parallel()

	backoff := &backoff{
		baseDelay: time.Second,
		maxDelay:  8 * time.Second,
		randomInt: func(limit int64) int64 { return limit - 1 },
	}

	delay0 := backoff.duration(0)
	delay1 := backoff.duration(1)
	delay2 := backoff.duration(2)
	delay6 := backoff.duration(6)

	if delay0 < time.Second || delay0 > 2*time.Second {
		t.Fatalf("delay0 = %s, want within [1s, 2s]", delay0)
	}
	if delay1 < time.Second || delay1 > 4*time.Second {
		t.Fatalf("delay1 = %s, want within [1s, 4s]", delay1)
	}
	if delay2 < 2*time.Second || delay2 > 8*time.Second {
		t.Fatalf("delay2 = %s, want within [2s, 8s]", delay2)
	}
	if delay6 < 4*time.Second || delay6 > 12*time.Second {
		t.Fatalf("delay6 = %s, want within [4s, 12s]", delay6)
	}
}
