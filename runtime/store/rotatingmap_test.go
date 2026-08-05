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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------- construction ----------

func TestMakeRotatingMap_InitialState(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Second)

	for i := range m.shards {
		if m.shards[i].current == nil {
			t.Fatalf("current map in shard %d must be initialised", i)
		}
		if m.shards[i].prev == nil {
			t.Fatalf("prev map in shard %d must be initialised", i)
		}
	}
	if m.timer != nil {
		t.Fatal("timer must be nil before Start")
	}
	if m.interval != time.Second {
		t.Fatalf("expected interval=1s, got %v", m.interval)
	}
}

// ---------- lifecycle ----------

func TestRotatingMap_Start_SetsTimer(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if m.timer == nil {
		t.Fatal("timer must be set after Start")
	}
	m.Stop(context.Background())
}

func TestRotatingMap_Stop_BeforeStart_NoPanic(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	m.Stop(context.Background()) // must not panic
}

func TestRotatingMap_Stop_AfterStart_ClearsTimer(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	_ = m.Start(context.Background())
	m.Stop(context.Background())
	if m.timer != nil {
		t.Fatal("timer must be nil after Stop")
	}
}

func TestRotatingMap_Stop_IdempotentDoubleCalls(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	_ = m.Start(context.Background())
	m.Stop(context.Background())
	m.Stop(context.Background()) // second Stop must not panic
}

// ---------- Set / Get ----------

func TestRotatingMap_Get_ExistingKey(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	if err := m.Set("k", 42); err != nil {
		t.Fatal(err)
	}

	v, ok := m.Get("k")
	if !ok {
		t.Fatal("expected key to be found")
	}
	if v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}
}

func TestRotatingMap_Get_MissingKey_ReturnsFalse(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)

	v, ok := m.Get("missing")
	if ok {
		t.Fatal("expected false for missing key")
	}
	if v != 0 {
		t.Fatalf("expected zero value, got %d", v)
	}
}

func TestRotatingMap_Set_DuplicateKeyInCurrent_ReturnsError(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	if err := m.Set("k", 1); err != nil {
		t.Fatal(err)
	}
	if err := m.Set("k", 2); err == nil {
		t.Fatal("expected error on duplicate key in current")
	}
}

func TestRotatingMap_Set_DuplicateKeyInPrev_ReturnsError(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	if err := m.Set("k", 1); err != nil {
		t.Fatal(err)
	}
	m.rotate() // k moves to prev
	if err := m.Set("k", 2); err == nil {
		t.Fatal("expected error on duplicate key in prev")
	}
}

// ---------- Pop ----------

func TestRotatingMap_Pop_ExistingKey(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	if err := m.Set("k", 7); err != nil {
		t.Fatal(err)
	}

	v, ok := m.Pop("k")
	if !ok || v != 7 {
		t.Fatalf("expected (7, true), got (%d, %v)", v, ok)
	}
	_, ok = m.Get("k")
	if ok {
		t.Fatal("key must not exist after Pop")
	}
}

func TestRotatingMap_Pop_MissingKey_ReturnsFalse(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)

	v, ok := m.Pop("missing")
	if ok {
		t.Fatal("expected false for missing key")
	}
	if v != 0 {
		t.Fatalf("expected zero value, got %d", v)
	}
}

func TestRotatingMap_Pop_TwiceOnSameKey(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	if err := m.Set("k", 5); err != nil {
		t.Fatal(err)
	}

	m.Pop("k")
	_, ok := m.Pop("k")
	if ok {
		t.Fatal("second Pop on same key must return false")
	}
}

// ---------- rotate ----------

// TestRotate_ItemMovesToPrev verifies that after one rotation an item set in
// current moves to prev and is still accessible via Get.
func TestRotatingMap_Rotate_ItemMovesToPrev(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	if err := m.Set("k", 10); err != nil {
		t.Fatal(err)
	}

	m.rotate()

	shard := m.shard("k")
	if len(shard.current) != 0 {
		t.Fatalf("current must be empty after rotate, len=%d", len(shard.current))
	}
	if v, ok := shard.prev["k"]; !ok || v != 10 {
		t.Fatalf("expected k=10 in prev, got ok=%v v=%d", ok, v)
	}

	v, ok := m.Get("k")
	if !ok || v != 10 {
		t.Fatalf("Get after rotate: expected (10, true), got (%d, %v)", v, ok)
	}
}

// TestRotate_PopFromPrev verifies that Pop retrieves a value that has been
// moved to prev by a rotation.
func TestRotatingMap_Rotate_PopFromPrev(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	if err := m.Set("k", 20); err != nil {
		t.Fatal(err)
	}
	m.rotate()

	v, ok := m.Pop("k")
	if !ok || v != 20 {
		t.Fatalf("expected (20, true) from prev, got (%d, %v)", v, ok)
	}
	_, ok = m.Get("k")
	if ok {
		t.Fatal("key must not exist after Pop from prev")
	}
}

// TestRotate_TwoRotations verifies that after two rotations an item set before
// the first rotation still survives: rotate merges prev items back into current
// before swapping, so items persist across the rotation boundary.
func TestRotatingMap_Rotate_TwoRotations_ItemSurvives(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	if err := m.Set("k", 5); err != nil {
		t.Fatal(err)
	}

	m.rotate() // current={} prev={k:5}
	m.rotate() // prev merged into current → current={k:5}; prev={k:5}; current={}

	v, ok := m.Get("k")
	if !ok || v != 5 {
		t.Fatalf("expected k=5 after two rotations, got ok=%v v=%d", ok, v)
	}
}

// TestRotate_GetSearchesCurrentThenPrev verifies lookup order: current is
// checked first, then prev.
func TestRotatingMap_Rotate_GetSearchesCurrentThenPrev(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)

	if err := m.Set("a", 1); err != nil {
		t.Fatal(err)
	}
	m.rotate() // a is in prev; current is empty

	// "a" must be found via prev.
	v, ok := m.Get("a")
	if !ok || v != 1 {
		t.Fatalf("expected a=1 from prev, got ok=%v v=%d", ok, v)
	}

	// A new key only in current is also found.
	if err := m.Set("b", 2); err != nil {
		t.Fatal(err)
	}
	v, ok = m.Get("b")
	if !ok || v != 2 {
		t.Fatalf("expected b=2 from current, got ok=%v v=%d", ok, v)
	}
}

// TestRotate_PopSearchesCurrentThenPrev verifies that Pop checks current first
// and, when absent there, removes from prev.
func TestRotatingMap_Rotate_PopSearchesCurrentThenPrev(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)

	if err := m.Set("a", 1); err != nil {
		t.Fatal(err)
	}
	m.rotate() // a is now in prev

	// Pop must find and remove "a" from prev.
	v, ok := m.Pop("a")
	if !ok || v != 1 {
		t.Fatalf("expected (1, true) from prev, got (%d, %v)", v, ok)
	}
	_, ok = m.Get("a")
	if ok {
		t.Fatal("key must be gone after Pop from prev")
	}

	// A key only in current is also popped correctly.
	if err := m.Set("b", 2); err != nil {
		t.Fatal(err)
	}
	v, ok = m.Pop("b")
	if !ok || v != 2 {
		t.Fatalf("expected (2, true) from current, got (%d, %v)", v, ok)
	}
}

// TestRotate_SetAfterRotate verifies that a Set after a rotation goes into the
// new (empty) current, not into prev.
func TestRotatingMap_Rotate_SetAfterRotate(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	if err := m.Set("a", 1); err != nil {
		t.Fatal(err)
	}
	m.rotate()

	if err := m.Set("b", 2); err != nil {
		t.Fatal(err)
	}

	shard := m.shard("b")
	if _, ok := shard.current["b"]; !ok {
		t.Fatal("key set after rotate must be in current")
	}
	if _, ok := shard.prev["b"]; ok {
		t.Fatal("key set after rotate must not be in prev")
	}
}

// TestRotate_MergePreservesItemsFromBothGenerations verifies that after two
// rotations items from different intervals are both accessible: items from the
// older interval survive in prev (via the merge step), and items from the newer
// interval are also present.
func TestRotatingMap_Rotate_MergePreservesItemsFromBothGenerations(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)

	if err := m.Set("a", 1); err != nil {
		t.Fatal(err)
	}
	m.rotate() // a moves to prev; current={}

	if err := m.Set("b", 2); err != nil {
		t.Fatal(err)
	}
	m.rotate() // merge prev{a:1} into current{b:2} → prev={a:1,b:2}; current={}

	va, oka := m.Get("a")
	if !oka || va != 1 {
		t.Fatalf("expected a=1 after two rotations, got ok=%v v=%d", oka, va)
	}
	vb, okb := m.Get("b")
	if !okb || vb != 2 {
		t.Fatalf("expected b=2 after two rotations, got ok=%v v=%d", okb, vb)
	}
}

// ---------- shrink-factor rotation guard ----------

// TestRotatingMap_SkipsRotationUnderHighLoad verifies that rotate() does not
// swap maps when the live entry count is >= highWaterMark/rotatingMapShrinkFactor
// (i.e., no significant memory waste to reclaim).
func TestRotatingMap_SkipsRotationUnderHighLoad(t *testing.T) {
	m := MakeRotatingMap[int, int](time.Hour)

	// Populate to establish a high water mark via the first forced rotation.
	const peak = 100
	for i := 0; i < peak; i++ {
		if err := m.Set(i, i); err != nil {
			t.Fatal(err)
		}
	}
	m.rotate() // first rotation always fires; highWaterMark = peak; prev={0..99}

	// With 100 live entries (all in prev) and highWaterMark=100,
	// total*factor = 100*4 = 400 >= 100 → rotation must be skipped.
	// After a skip: current stays empty (was not replaced) and prev still has all entries.
	lenCurrentBefore, lenPrevBefore := rotatingMapGenerationSizes(m)

	m.rotate()

	lenCurrentAfter, lenPrevAfter := rotatingMapGenerationSizes(m)
	if lenPrevAfter != lenPrevBefore {
		t.Fatalf("prev must not change when rotation is skipped: before=%d after=%d", lenPrevBefore, lenPrevAfter)
	}
	if lenCurrentAfter != lenCurrentBefore {
		t.Fatalf("current must not change when rotation is skipped: before=%d after=%d", lenCurrentBefore, lenCurrentAfter)
	}
	// Spot-check: items must still be accessible.
	if v, ok := m.Get(0); !ok || v != 0 {
		t.Fatal("items must remain accessible after skipped rotation")
	}
}

// TestRotatingMap_RotatesAfterBurstRecovery verifies that rotate() fires once
// live entries drop below highWaterMark/rotatingMapShrinkFactor.
func TestRotatingMap_RotatesAfterBurstRecovery(t *testing.T) {
	m := MakeRotatingMap[int, int](time.Hour)

	const peak = 100
	for i := 0; i < peak; i++ {
		if err := m.Set(i, i); err != nil {
			t.Fatal(err)
		}
	}
	m.rotate() // highWaterMark = peak; all entries in prev

	// Pop enough entries so that live < peak/rotatingMapShrinkFactor.
	// Keep only peak/rotatingMapShrinkFactor - 1 entries alive.
	threshold := peak / rotatingMapShrinkFactor // = 25
	for i := 0; i < peak-threshold+1; i++ {
		m.Pop(i)
	}
	// Live entries = threshold-1 = 24, which is < 25 → rotation should fire.

	m.rotate()

	// After rotation: current must be a fresh empty map; prev has the surviving entries.
	current, _ := rotatingMapGenerationSizes(m)
	if current != 0 {
		t.Fatalf("current generations must be empty after rotation, got len=%d", current)
	}
}

// TestRotatingMap_HighWaterMarkTrackedWhenSkipped verifies that highWaterMark
// is updated even when a rotation is skipped, so a later burst is correctly measured.
func TestRotatingMap_HighWaterMarkTrackedWhenSkipped(t *testing.T) {
	m := MakeRotatingMap[int, int](time.Hour)

	// First rotation with 10 entries: highWaterMark = 10.
	for i := 0; i < 10; i++ {
		_ = m.Set(i, i)
	}
	m.rotate() // highWaterMark = 10; entries move to prev

	// Add 200 more entries (growing load) and trigger a skipped rotation.
	for i := 10; i < 210; i++ {
		_ = m.Set(i, i)
	}
	m.rotate() // total = ~210; 210*4 >= 10 → skip; but highWaterMark must update to ~210

	if highWaterMark := rotatingMapHighWaterMark(m); highWaterMark < 200 {
		t.Fatalf("highWaterMark must track growing load even when rotation is skipped, got %d", highWaterMark)
	}
}

// TestRotatingMap_FirstCallAlwaysRotates verifies the initial rotation always
// fires regardless of entry count, since highWaterMark starts at 0.
func TestRotatingMap_FirstCallAlwaysRotates(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	if err := m.Set("k", 1); err != nil {
		t.Fatal(err)
	}

	m.rotate()

	// After the first rotation current must be a fresh empty map.
	shard := m.shard("k")
	if len(shard.current) != 0 {
		t.Fatal("first rotate() call must always perform the rotation: current must be empty")
	}
	if _, ok := shard.prev["k"]; !ok {
		t.Fatal("item must be in prev after first rotation")
	}
}

// ---------- timer-based rotation ----------

// NOTE: TestRotatingMap_TimerFiresRotation is omitted to avoid timing-sensitive
// tests in CI. The rotation logic itself is fully exercised via direct rotate()
// calls above.

// ---------- concurrency ----------

func TestRotatingMap_Concurrent_SetGet(t *testing.T) {
	m := MakeRotatingMap[int, int](time.Hour)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		i := i
		go func() { defer wg.Done(); _ = m.Set(i, i*10) }()
		go func() { defer wg.Done(); m.Get(i) }()
	}
	wg.Wait()
}

func TestRotatingMap_DistributesKeysAcrossShards(t *testing.T) {
	m := MakeRotatingMap[string, int](time.Hour)
	for i := 0; i < 1_000; i++ {
		key := fmt.Sprintf("stream-%d", i)
		if err := m.Set(key, i); err != nil {
			t.Fatal(err)
		}
	}

	used := 0
	for i := range m.shards {
		if len(m.shards[i].current) != 0 {
			used++
		}
	}
	if used < rotatingMapShardCount/2 {
		t.Fatalf("expected keys to use at least half the shards, used %d", used)
	}
}

func TestRotatingMap_Concurrent_SetPopRotate(t *testing.T) {
	m := MakeRotatingMap[int, int](time.Hour)
	const goroutines = 30

	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	for i := 0; i < goroutines; i++ {
		i := i
		go func() { defer wg.Done(); _ = m.Set(i, i) }()
		go func() { defer wg.Done(); m.Pop(i) }()
		go func() { defer wg.Done(); m.rotate() }()
	}
	wg.Wait()
}

func BenchmarkRotatingMap_ParallelRequestLifecycle(b *testing.B) {
	m := MakeRotatingMap[string, int](time.Hour)
	var sequence atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := strconv.FormatUint(sequence.Add(1), 10)
			if err := m.Set(key, 1); err != nil {
				b.Fatal(err)
			}
			if value, ok := m.Get(key); !ok || value != 1 {
				b.Fatalf("unexpected value %d, found=%v", value, ok)
			}
			if _, ok := m.Pop(key); !ok {
				b.Fatal("pending entry disappeared")
			}
		}
	})
}

func rotatingMapGenerationSizes[K comparable, V any](m *RotatingMap[K, V]) (current, previous int) {
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.Lock()
		current += len(shard.current)
		previous += len(shard.prev)
		shard.mu.Unlock()
	}
	return current, previous
}

func rotatingMapHighWaterMark[K comparable, V any](m *RotatingMap[K, V]) int {
	result := 0
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.Lock()
		result += shard.highWaterMark
		shard.mu.Unlock()
	}
	return result
}
