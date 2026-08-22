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
	"hash/maphash"
	"sync"
	"time"
)

// Each shard must first reach defaultRotatingMapMinCapacity entries before it
// is eligible for rotation. rotatingMapShrinkFactor then controls when an
// eligible shard is actually compacted.
// Rotation is skipped when live entry count >= highWaterMark/rotatingMapShrinkFactor,
// i.e. rotation fires only when current usage has dropped below 25% of the peak.
// This avoids pointless rotations under steady or growing load while still reclaiming
// memory after burst traffic.
const rotatingMapShrinkFactor = 4
const rotatingMapShardCount = 64
const defaultRotatingMapMinCapacity = 1_000

type rotatingMapShard[K comparable, V any] struct {
	current       map[K]V
	prev          map[K]V
	mu            sync.Mutex
	highWaterMark int
}

type RotatingMap[K comparable, V any] struct {
	shards      [rotatingMapShardCount]rotatingMapShard[K, V]
	interval    time.Duration
	lifecycleMu sync.Mutex
	timer       *time.Timer
	seed        maphash.Seed
	minCapacity int
}

func MakeRotatingMap[K comparable, V any](interval time.Duration) *RotatingMap[K, V] {
	return makeRotatingMap[K, V](interval, defaultRotatingMapMinCapacity)
}

func makeRotatingMap[K comparable, V any](interval time.Duration, minCapacity int) *RotatingMap[K, V] {
	m := &RotatingMap[K, V]{interval: interval, seed: maphash.MakeSeed(), minCapacity: minCapacity}
	for i := range m.shards {
		m.shards[i].current = make(map[K]V)
		m.shards[i].prev = make(map[K]V)
	}
	return m
}

func (m *RotatingMap[K, V]) Start(_ context.Context) error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	m.timer = time.AfterFunc(m.interval, m.rotate)
	return nil
}

func (m *RotatingMap[K, V]) Stop(_ context.Context) {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.timer != nil {
		m.timer.Stop()
		m.timer = nil
	}
}

func (m *RotatingMap[K, V]) Set(key K, value V) error {
	shard := m.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if _, exists := shard.current[key]; exists {
		return fmt.Errorf("duplicate stream ID %v", key)
	}
	if _, exists := shard.prev[key]; exists {
		return fmt.Errorf("duplicate stream ID %v", key)
	}
	shard.current[key] = value
	return nil
}

// GetOrCreate returns the existing value for key if present (checking current
// then prev, without moving it). Otherwise it atomically creates one via
// factory and stores it in current. loaded reports whether an existing value
// was found. factory must be cheap and non-blocking: it runs while the shard's
// lock is held, so callers must not perform I/O or other blocking work inside it.
func (m *RotatingMap[K, V]) GetOrCreate(key K, factory func() V) (value V, loaded bool) {
	shard := m.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if v, exists := shard.current[key]; exists {
		return v, true
	}
	if v, exists := shard.prev[key]; exists {
		return v, true
	}
	v := factory()
	shard.current[key] = v
	return v, false
}

func (m *RotatingMap[K, V]) Get(key K) (V, bool) {
	shard := m.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if v, exists := shard.current[key]; exists {
		return v, true
	}
	if v, exists := shard.prev[key]; exists {
		return v, true
	}
	var zero V
	return zero, false
}

func (m *RotatingMap[K, V]) Pop(key K) (V, bool) {
	shard := m.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if v, exists := shard.current[key]; exists {
		delete(shard.current, key)
		return v, true
	}
	if v, exists := shard.prev[key]; exists {
		delete(shard.prev, key)
		return v, true
	}
	var zero V
	return zero, false
}

func (m *RotatingMap[K, V]) rotate() {
	for i := range m.shards {
		m.rotateShard(&m.shards[i])
	}
	m.resetTimer()
}

func (m *RotatingMap[K, V]) resetTimer() {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.timer != nil {
		m.timer.Reset(m.interval)
	}
}

func (m *RotatingMap[K, V]) rotateShard(shard *rotatingMapShard[K, V]) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	total := len(shard.current) + len(shard.prev)
	shouldRotate := shard.highWaterMark == 0 || total*rotatingMapShrinkFactor < shard.highWaterMark

	if total > shard.highWaterMark {
		shard.highWaterMark = total
	}
	if shard.highWaterMark < m.minCapacity {
		return
	}
	if !shouldRotate {
		return
	}

	shard.highWaterMark = total
	newMap := make(map[K]V)
	for k, v := range shard.prev {
		shard.current[k] = v
	}
	shard.prev = shard.current
	shard.current = newMap
}

func (m *RotatingMap[K, V]) shard(key K) *rotatingMapShard[K, V] {
	return &m.shards[maphash.Comparable(m.seed, key)%rotatingMapShardCount]
}
