package fanout

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryPublishAndSubscribe(t *testing.T) {
	t.Parallel()

	bus := NewMemory[string]()

	sub, cancel, err := bus.Subscribe(context.Background(), "us")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	if err := bus.Publish(context.Background(), "us", "hello"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-sub:
		if got != "hello" {
			t.Fatalf("got %q, want hello", got)
		}
	default:
		t.Fatal("expected outbound message")
	}
}

func TestMemoryPublishWaitsForContextWhenSubscriberBufferIsFull(t *testing.T) {
	t.Parallel()

	bus := NewMemory[string]()

	sub, cancelSub, err := bus.Subscribe(context.Background(), "us")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancelSub()

	for i := 0; i < defaultBufferSize; i++ {
		if err := bus.Publish(context.Background(), "us", "ping"); err != nil {
			t.Fatalf("fill buffer: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err = bus.Publish(ctx, "us", "ping")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	select {
	case <-sub:
	default:
		t.Fatal("expected buffered messages to remain queued")
	}
}
