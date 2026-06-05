/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type RotatingMap[K comparable, V any] struct {
	current  map[K]V
	prev     map[K]V
	mu       sync.Mutex
	interval time.Duration
	timer    *time.Timer
}

func MakeRotatingMap[K comparable, V any](interval time.Duration) *RotatingMap[K, V] {
	return &RotatingMap[K, V]{
		current:  make(map[K]V),
		prev:     make(map[K]V),
		interval: interval,
	}
}

func (m *RotatingMap[K, V]) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timer = time.AfterFunc(m.interval, m.rotate)
	return nil
}

func (m *RotatingMap[K, V]) Stop(_ context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.timer != nil {
		m.timer.Stop()
		m.timer = nil
	}
}

func (m *RotatingMap[K, V]) Set(key K, value V) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.current[key]; exists {
		return fmt.Errorf("duplicate stream ID %v", key)
	}
	if _, exists := m.prev[key]; exists {
		return fmt.Errorf("duplicate stream ID %v", key)
	}
	m.current[key] = value
	return nil
}

// Get retrieves the value without deleting it. Returns false if not found.
func (m *RotatingMap[K, V]) Get(key K) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, exists := m.current[key]; exists {
		return v, true
	}
	if v, exists := m.prev[key]; exists {
		return v, true
	}
	var zero V
	return zero, false
}

// Pop atomically retrieves and deletes the value. Returns false if not found.
func (m *RotatingMap[K, V]) Pop(key K) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, exists := m.current[key]; exists {
		delete(m.current, key)
		return v, true
	}
	if v, exists := m.prev[key]; exists {
		delete(m.prev, key)
		return v, true
	}
	var zero V
	return zero, false
}

func (m *RotatingMap[K, V]) rotate() {
	newMap := make(map[K]V)
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.prev {
		m.current[k] = v
	}
	m.prev = m.current
	m.current = newMap
	if m.timer != nil {
		m.timer.Reset(m.interval)
	}
}
