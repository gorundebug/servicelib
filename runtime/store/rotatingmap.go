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

// rotatingMapShrinkFactor controls when rotation is skipped.
// Rotation is skipped when live entry count >= highWaterMark/rotatingMapShrinkFactor,
// i.e. rotation fires only when current usage has dropped below 25% of the peak.
// This avoids pointless rotations under steady or growing load while still reclaiming
// memory after burst traffic.
const rotatingMapShrinkFactor = 4

type RotatingMap[K comparable, V any] struct {
	current       map[K]V
	prev          map[K]V
	mu            sync.Mutex
	interval      time.Duration
	timer         *time.Timer
	highWaterMark int // max live entries observed since last rotation
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
	m.mu.Lock()
	defer m.mu.Unlock()

	total := len(m.current) + len(m.prev)

	// Compute before updating highWaterMark so the first call (highWaterMark==0) always rotates.
	shouldRotate := m.highWaterMark == 0 || total*rotatingMapShrinkFactor < m.highWaterMark

	// Track high water mark across all intervals (including skipped ones).
	if total > m.highWaterMark {
		m.highWaterMark = total
	}

	if shouldRotate {
		// Reset water mark so the next burst is measured from a clean baseline.
		m.highWaterMark = total

		newMap := make(map[K]V)
		for k, v := range m.prev {
			m.current[k] = v
		}
		m.prev = m.current
		m.current = newMap
	}

	if m.timer != nil {
		m.timer.Reset(m.interval)
	}
}
