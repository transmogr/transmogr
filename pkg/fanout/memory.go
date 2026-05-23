// Package fanout provides keyed in-memory publish/subscribe primitives.
package fanout

import (
	"context"
	"sync"
)

const defaultBufferSize = 128

// Bus fan-outs messages to subscribers registered for a specific key.
type Bus[T any] interface {
	Subscribe(ctx context.Context, key string) (<-chan T, func(), error)
	Publish(ctx context.Context, key string, message T) error
}

// Memory is an in-memory keyed pub/sub implementation.
type Memory[T any] struct {
	mu         sync.Mutex
	nextID     uint64
	bufferSize int
	subs       map[string]map[uint64]chan T
}

// NewMemory creates an in-memory bus with the default subscriber buffer size.
func NewMemory[T any]() *Memory[T] {
	return NewMemoryWithBuffer[T](defaultBufferSize)
}

// NewMemoryWithBuffer creates an in-memory bus with a custom subscriber buffer size.
func NewMemoryWithBuffer[T any](bufferSize int) *Memory[T] {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}

	return &Memory[T]{
		bufferSize: bufferSize,
		subs:       make(map[string]map[uint64]chan T),
	}
}

// Subscribe registers a keyed consumer.
func (m *Memory[T]) Subscribe(_ context.Context, key string) (<-chan T, func(), error) {
	m.mu.Lock()
	m.nextID++
	id := m.nextID
	ch := make(chan T, m.bufferSize)
	if m.subs[key] == nil {
		m.subs[key] = make(map[uint64]chan T)
	}
	m.subs[key][id] = ch
	m.mu.Unlock()

	return ch, func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		if m.subs[key] == nil {
			return
		}
		if _, ok := m.subs[key][id]; !ok {
			return
		}

		delete(m.subs[key], id)
		close(ch)
		if len(m.subs[key]) == 0 {
			delete(m.subs, key)
		}
	}, nil
}

// Publish broadcasts a message to all subscribers for the target key.
func (m *Memory[T]) Publish(ctx context.Context, key string, message T) error {
	m.mu.Lock()
	targets := make([]chan T, 0, len(m.subs[key]))
	for _, ch := range m.subs[key] {
		targets = append(targets, ch)
	}
	m.mu.Unlock()

	for _, ch := range targets {
		select {
		case ch <- message:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
