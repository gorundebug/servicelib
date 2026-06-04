/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

// ---------- mocks ----------

type mockGauge struct {
	count atomic.Int64
}

func (g *mockGauge) Set(v int64)     { g.count.Store(v) }
func (g *mockGauge) Inc()            { g.count.Add(1) }
func (g *mockGauge) Dec()            { g.count.Add(-1) }
func (g *mockGauge) Add(delta int64) { g.count.Add(delta) }
func (g *mockGauge) Sub(delta int64) { g.count.Add(-delta) }
func (g *mockGauge) Value() int64    { return g.count.Load() }

var _ metrics.Int64Gauge = (*mockGauge)(nil)

type mockMetrics struct {
	gauge *mockGauge
}

func newMockMetrics() *mockMetrics { return &mockMetrics{gauge: &mockGauge{}} }

func (m *mockMetrics) Scope(_ string, _ metrics.Labels) metrics.MetricsScope {
	return &mockMetricsScope{m: m}
}

type mockMetricsScope struct {
	m *mockMetrics
}

func (s *mockMetricsScope) Counter(_, _ string, _ metrics.Labels) (metrics.Int64Counter, error) {
	return &mockCounter{}, nil
}
func (s *mockMetricsScope) CounterVec(_, _ string) (metrics.Int64CounterVec, error) {
	return nil, nil
}
func (s *mockMetricsScope) Gauge(_, _ string, _ metrics.Labels) (metrics.Int64Gauge, error) {
	return s.m.gauge, nil
}
func (s *mockMetricsScope) GaugeVec(_, _ string) (metrics.Int64GaugeVec, error) {
	return nil, nil
}
func (s *mockMetricsScope) Histogram(_, _ string, _ metrics.Labels, _ ...float64) (metrics.Float64Histogram, error) {
	return nil, nil
}
func (s *mockMetricsScope) HistogramVec(_, _ string, _ ...float64) (metrics.Float64HistogramVec, error) {
	return nil, nil
}
func (s *mockMetricsScope) ObservableFloat64Gauge(_, _ string, _ func() float64) error {
	return nil
}

type mockCounter struct{}

func (c *mockCounter) Inc(_ context.Context)          {}
func (c *mockCounter) Add(_ context.Context, _ int64) {}

type mockLogger struct{}

func (l *mockLogger) Debugf(_ string, _ ...interface{})   {}
func (l *mockLogger) Infof(_ string, _ ...interface{})    {}
func (l *mockLogger) Printf(_ string, _ ...interface{})   {}
func (l *mockLogger) Warnf(_ string, _ ...interface{})    {}
func (l *mockLogger) Warningf(_ string, _ ...interface{}) {}
func (l *mockLogger) Errorf(_ string, _ ...interface{})   {}
func (l *mockLogger) Fatalf(_ string, _ ...interface{})   {}
func (l *mockLogger) Panicf(_ string, _ ...interface{})   {}
func (l *mockLogger) Debug(_ ...interface{})              {}
func (l *mockLogger) Info(_ ...interface{})               {}
func (l *mockLogger) Print(_ ...interface{})              {}
func (l *mockLogger) Warn(_ ...interface{})               {}
func (l *mockLogger) Warning(_ ...interface{})            {}
func (l *mockLogger) Error(_ ...interface{})              {}
func (l *mockLogger) Fatal(_ ...interface{})              {}
func (l *mockLogger) Panic(_ ...interface{})              {}
func (l *mockLogger) Debugln(_ ...interface{})            {}
func (l *mockLogger) Infoln(_ ...interface{})             {}
func (l *mockLogger) Println(_ ...interface{})            {}
func (l *mockLogger) Warnln(_ ...interface{})             {}
func (l *mockLogger) Warningln(_ ...interface{})          {}
func (l *mockLogger) Errorln(_ ...interface{})            {}
func (l *mockLogger) Fatalln(_ ...interface{})            {}
func (l *mockLogger) Panicln(_ ...interface{})            {}
func (l *mockLogger) NativeLogger() interface{}           { return nil }

type mockServiceEnvironment struct {
	serviceCfg *config.ServiceConfig
	m          metrics.Metrics
}

func newMockEnv(name string, m metrics.Metrics) *mockServiceEnvironment {
	return &mockServiceEnvironment{
		serviceCfg: &config.ServiceConfig{Name: name},
		m:          m,
	}
}

func (e *mockServiceEnvironment) ServiceConfig() *config.ServiceConfig                 { return e.serviceCfg }
func (e *mockServiceEnvironment) Metrics() metrics.Metrics                             { return e.m }
func (e *mockServiceEnvironment) Tracing() tracing.Tracing                             { return nil }
func (e *mockServiceEnvironment) RuntimeConfig() *config.RuntimeConfig                 { return nil }
func (e *mockServiceEnvironment) Log() log.Logger                                      { return &mockLogger{} }
func (e *mockServiceEnvironment) ServiceDependencies() environment.ServiceDependencies { return nil }
func (e *mockServiceEnvironment) ServiceContext() interface{}                          { return nil }

type mockJoinStorageConfig struct {
	name     string
	ttl      time.Duration
	renewTTL bool
}

func (c *mockJoinStorageConfig) GetName() string       { return c.name }
func (c *mockJoinStorageConfig) GetTTL() time.Duration { return c.ttl }
func (c *mockJoinStorageConfig) GetRenewTTL() bool     { return c.renewTTL }

// ---------- helpers ----------

func makeStorage(t *testing.T, ttl time.Duration, renewTTL bool) (*HashMapJoinStorage[string], *mockGauge) {
	t.Helper()
	m := newMockMetrics()
	env := newMockEnv("test-service", m)
	cfg := &mockJoinStorageConfig{name: "test-join", ttl: ttl, renewTTL: renewTTL}
	storage, err := MakeHashMapJoinStorage[string](env, cfg)
	if err != nil {
		t.Fatalf("MakeHashMapJoinStorage: %v", err)
	}
	if err := storage.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	s := storage.(*HashMapJoinStorage[string])
	return s, m.gauge
}

// ---------- construction tests ----------

func TestMakeHashMapJoinStorage_NoTTL(t *testing.T) {
	s, _ := makeStorage(t, 0, false)
	if s.storage1 == nil {
		t.Fatal("storage1 must be initialised")
	}
	if s.storage2 != nil {
		t.Fatal("storage2 must be nil when TTL=0")
	}
	if s.timer != nil {
		t.Fatal("timer must be nil when TTL=0")
	}
}

func TestMakeHashMapJoinStorage_WithTTL(t *testing.T) {
	s, _ := makeStorage(t, 100*time.Millisecond, false)
	defer s.Stop(context.Background())
	if s.storage2 == nil {
		t.Fatal("storage2 must be initialised when TTL>0")
	}
	if s.timer == nil {
		t.Fatal("timer must be set when TTL>0")
	}
}

// ---------- lifecycle tests ----------

func TestStart_ReturnsNil(t *testing.T) {
	s, _ := makeStorage(t, 0, false)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

func TestStop_NoTimer(t *testing.T) {
	s, _ := makeStorage(t, 0, false)
	s.Stop(context.Background()) // must not panic
}

func TestStop_WithTimer(t *testing.T) {
	s, _ := makeStorage(t, time.Hour, false)
	s.Stop(context.Background()) // must not panic and should stop the timer
}

// ---------- JoinValue (TTL=0) tests ----------

// TestJoinValue_NoTTL_CallbackReceivesValue verifies that the callback is called
// and receives the value passed to JoinValue.
func TestJoinValue_NoTTL_CallbackReceivesValue(t *testing.T) {
	s, _ := makeStorage(t, 0, false)

	called := false
	s.JoinValue(context.Background(), "k1", 0, "hello", func(values [][]interface{}) bool {
		called = true
		if len(values) < 1 || len(values[0]) != 1 || values[0][0] != "hello" {
			t.Errorf("unexpected values: %v", values)
		}
		return false
	})

	if !called {
		t.Fatal("callback was not called")
	}
}

// TestJoinValue_NoTTL_CallbackTrue_RemovesItem verifies that when the callback
// returns true the item is deleted from storage and the gauge is decremented.
func TestJoinValue_NoTTL_CallbackTrue_RemovesItem(t *testing.T) {
	s, gauge := makeStorage(t, 0, false)

	s.JoinValue(context.Background(), "k1", 0, 42, func(values [][]interface{}) bool {
		return true
	})

	if _, ok := s.storage1["k1"]; ok {
		t.Fatal("item must be removed from storage when callback returns true")
	}
	if gauge.Value() != 0 {
		t.Fatalf("expected gauge=0 after removal, got %d", gauge.Value())
	}
}

// TestJoinValue_NoTTL_CallbackFalse_ItemPersists verifies that when the callback
// returns false the item remains in storage.
func TestJoinValue_NoTTL_CallbackFalse_ItemPersists(t *testing.T) {
	s, gauge := makeStorage(t, 0, false)

	s.JoinValue(context.Background(), "k1", 0, "v", func(values [][]interface{}) bool {
		return false
	})

	if _, ok := s.storage1["k1"]; !ok {
		t.Fatal("item must remain when callback returns false")
	}
	if gauge.Value() != 1 {
		t.Fatalf("expected gauge=1, got %d", gauge.Value())
	}
}

// TestJoinValue_NoTTL_GaugeIncrementedOnFirstInsert verifies that the gauge is
// incremented only when a key is first inserted, not on subsequent calls.
// NOTE: with TTL=0 the deadline is the zero time, so the existing-item lookup
// (time.Now().Before(zero)) is always false; subsequent calls overwrite the
// item in storage without incrementing the gauge again.
func TestJoinValue_NoTTL_GaugeIncrementedOnFirstInsert(t *testing.T) {
	s, gauge := makeStorage(t, 0, false)

	// First call: key is new → gauge goes to 1.
	s.JoinValue(context.Background(), "k1", 0, "a", func(values [][]interface{}) bool { return false })
	if gauge.Value() != 1 {
		t.Fatalf("expected gauge=1 after first insert, got %d", gauge.Value())
	}

	// Second call: key exists but deadline=zero causes the item to be replaced
	// without incrementing the gauge.
	s.JoinValue(context.Background(), "k1", 0, "b", func(values [][]interface{}) bool { return false })
	if gauge.Value() != 1 {
		t.Fatalf("expected gauge still=1 after second call, got %d", gauge.Value())
	}
}

// TestJoinValue_NoTTL_MultipleKeys_GaugeTracksAll verifies the gauge counts
// distinct keys independently.
func TestJoinValue_NoTTL_MultipleKeys_GaugeTracksAll(t *testing.T) {
	s, gauge := makeStorage(t, 0, false)

	for _, key := range []string{"a", "b", "c"} {
		s.JoinValue(context.Background(), key, 0, 1, func(values [][]interface{}) bool { return false })
	}
	if gauge.Value() != 3 {
		t.Fatalf("expected gauge=3 for 3 distinct keys, got %d", gauge.Value())
	}
}

// TestJoinValue_NoTTL_CallbackTrue_DecrementsThenNextInsertsIncrement verifies
// that after an item is removed (callback=true) a subsequent call for the same
// key starts fresh and increments the gauge again.
func TestJoinValue_NoTTL_CallbackTrue_DecrementsThenNextInsertsIncrement(t *testing.T) {
	s, gauge := makeStorage(t, 0, false)

	s.JoinValue(context.Background(), "k1", 0, "v1", func(values [][]interface{}) bool { return true })
	if gauge.Value() != 0 {
		t.Fatalf("gauge should be 0 after completion, got %d", gauge.Value())
	}

	s.JoinValue(context.Background(), "k1", 0, "v2", func(values [][]interface{}) bool { return false })
	if gauge.Value() != 1 {
		t.Fatalf("gauge should be 1 after re-insert, got %d", gauge.Value())
	}
}

// TestJoinValue_NoTTL_IndexOne verifies that JoinValue with index=1 works
// (values slice has two slots: [nil, [value]]).
func TestJoinValue_NoTTL_IndexOne(t *testing.T) {
	s, _ := makeStorage(t, 0, false)

	s.JoinValue(context.Background(), "k1", 1, "v", func(values [][]interface{}) bool {
		if len(values) < 2 {
			t.Errorf("expected at least 2 slots, got %d", len(values))
		} else if len(values[1]) != 1 || values[1][0] != "v" {
			t.Errorf("unexpected value at index 1: %v", values[1])
		}
		return false
	})
}

// TestJoinValue_NoTTL_Concurrent_NoPanic verifies that concurrent JoinValue
// calls do not panic or produce data races.
func TestJoinValue_NoTTL_Concurrent_NoPanic(t *testing.T) {
	s, _ := makeStorage(t, 0, false)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s.JoinValue(context.Background(), "shared", 0, 1, func(values [][]interface{}) bool {
				return false
			})
		}()
	}
	wg.Wait()
}

// TestJoinValue_NoTTL_Concurrent_MultipleKeys verifies concurrent writes to
// different keys are all recorded independently.
func TestJoinValue_NoTTL_Concurrent_MultipleKeys(t *testing.T) {
	s, gauge := makeStorage(t, 0, false)

	const keys = 50
	var wg sync.WaitGroup
	wg.Add(keys)
	for i := 0; i < keys; i++ {
		key := string(rune('A' + i))
		go func(k string) {
			defer wg.Done()
			s.JoinValue(context.Background(), k, 0, 1, func(values [][]interface{}) bool { return false })
		}(key)
	}
	wg.Wait()

	if gauge.Value() != keys {
		t.Fatalf("expected gauge=%d, got %d", keys, gauge.Value())
	}
}

// TestJoinValue_NoTTL_ProcessedFlag_NoReprocess verifies that after an item is
// processed and deleted, the next JoinValue call creates a fresh item and
// invokes f again.
func TestJoinValue_NoTTL_ProcessedFlag_NoReprocess(t *testing.T) {
	s, _ := makeStorage(t, 0, false)

	callCount := atomic.Int32{}

	// First call: f returns true → item is deleted from storage.
	s.JoinValue(context.Background(), "k1", 0, "v1", func(values [][]interface{}) bool {
		callCount.Add(1)
		return true
	})

	// Second call: previous item was deleted, so a fresh item is created and f
	// is invoked again.
	s.JoinValue(context.Background(), "k1", 0, "v2", func(values [][]interface{}) bool {
		callCount.Add(1)
		return true
	})

	if callCount.Load() != 2 {
		t.Fatalf("expected 2 callback invocations, got %d", callCount.Load())
	}
}

// ---------- rotate tests ----------

func TestRotate_MovesStorage1ToStorage2(t *testing.T) {
	s, gauge := makeStorage(t, time.Hour, false)
	defer s.Stop(context.Background())

	// Manually populate storage1.
	s.storage1["k1"] = &Item{values: make([][]interface{}, 1)}
	gauge.Inc()

	s.rotate(context.Background())

	if len(s.storage2) != 1 {
		t.Fatalf("storage2 should have 1 item after rotate, got %d", len(s.storage2))
	}
	if len(s.storage1) != 0 {
		t.Fatalf("storage1 should be empty after rotate, got %d", len(s.storage1))
	}
	if _, ok := s.storage2["k1"]; !ok {
		t.Fatal("k1 should be in storage2 after rotate")
	}
}

func TestRotate_DecrementsGaugeForEvictedItems(t *testing.T) {
	s, gauge := makeStorage(t, time.Hour, false)
	defer s.Stop(context.Background())

	// Pre-populate storage2 (items evicted on the next rotate).
	s.storage2["old1"] = &Item{}
	s.storage2["old2"] = &Item{}
	gauge.Add(2)

	s.rotate(context.Background())

	if gauge.Value() != 0 {
		t.Fatalf("gauge should be 0 after evicting 2 items, got %d", gauge.Value())
	}
}

// ---------- JoinValue (TTL > 0) tests ----------

// TestJoinValue_WithTTL_CallbackInvoked uses a 1 hour TTL to keep the item
// alive during the call, covering the TTL > 0 code paths:
// rotateLock.RLock (line 75), newItem.deadline assignment (line 111), and
// the callback being called while the item is alive (line 123).
func TestJoinValue_WithTTL_CallbackInvoked(t *testing.T) {
	s, gauge := makeStorage(t, time.Hour, false)
	defer s.Stop(context.Background())

	called := false
	s.JoinValue(context.Background(), "k1", 0, "val", func(values [][]interface{}) bool {
		called = true
		if len(values) == 0 || len(values[0]) == 0 || values[0][0] != "val" {
			t.Errorf("unexpected values: %v", values)
		}
		return true
	})

	if !called {
		t.Fatal("callback was not called with TTL > 0")
	}
	if gauge.Value() != 0 {
		t.Fatalf("expected gauge=0 after removal, got %d", gauge.Value())
	}
}

// TestJoinValue_WithTTL_CallbackFalse_Persists verifies that when the callback
// returns false the item is NOT deleted.
func TestJoinValue_WithTTL_CallbackFalse_Persists(t *testing.T) {
	s, gauge := makeStorage(t, time.Hour, false)
	defer s.Stop(context.Background())

	s.JoinValue(context.Background(), "k1", 0, "v", func(values [][]interface{}) bool {
		return false
	})

	if gauge.Value() != 1 {
		t.Fatalf("expected gauge=1 after false callback, got %d", gauge.Value())
	}
}

// TestJoinValue_WithTTL_MultipleKeys_CallbacksInvoked verifies that JoinValue
// with TTL > 0 invokes the callback for each distinct key.
func TestJoinValue_WithTTL_MultipleKeys_CallbacksInvoked(t *testing.T) {
	s, _ := makeStorage(t, time.Hour, false)
	defer s.Stop(context.Background())

	for _, k := range []string{"a", "b", "c"} {
		called := false
		s.JoinValue(context.Background(), k, 0, 1, func(values [][]interface{}) bool {
			called = true
			return false
		})
		if !called {
			t.Errorf("callback not called for key %q", k)
		}
	}
}

// TestJoinValue_RenewTTL_ExecutesRenewalPath covers the renewTTL branch
// (lines 134-141): callback returns false with renewTTL=true, so the item
// deadline is extended and the item is kept in storage1.
func TestJoinValue_RenewTTL_ExecutesRenewalPath(t *testing.T) {
	s, gauge := makeStorage(t, time.Hour, true /* renewTTL */)
	defer s.Stop(context.Background())

	called := false
	s.JoinValue(context.Background(), "k1", 0, "v", func(values [][]interface{}) bool {
		called = true
		return false // trigger renewTTL branch
	})

	if !called {
		t.Fatal("callback not called")
	}
	// item must still be present (renewTTL keeps it alive)
	if gauge.Value() != 1 {
		t.Fatalf("expected gauge=1 after renewTTL, got %d", gauge.Value())
	}
}

// TestJoinValue_ItemFoundInStorage2 covers the storage2 lookup path
// (lines 88-93). We manually pre-insert an item into storage2 with a 1 ms
// deadline so that the read lock finds it there for several loop iterations
// before the deadline expires and a fresh item is created in storage1.
func TestJoinValue_ItemFoundInStorage2(t *testing.T) {
	s, _ := makeStorage(t, time.Nanosecond, false)
	defer s.Stop(context.Background())

	// Insert directly into storage2 with a 1 ms future deadline so the read
	// path hits lines 90-92 (item != nil && time.Now().Before(deadline)).
	// rotateLock.RLock prevents rotate() from swapping / len()-ing storage2
	// concurrently with our direct map write.
	s.rotateLock.RLock()
	s.lock.Lock()
	s.storage2["k2"] = &Item{
		values:   make([][]interface{}, 1),
		deadline: time.Now().Add(time.Millisecond),
	}
	s.lock.Unlock()
	s.rotateLock.RUnlock()

	// JoinValue will spin (~1 ms) finding the storage2 item each iteration
	// (covering lines 88-93), then after it expires a new item is created and
	// the callback is invoked.
	called := false
	s.JoinValue(context.Background(), "k2", 0, "val", func(values [][]interface{}) bool {
		called = true
		return true
	})

	if !called {
		t.Fatal("callback not called after storage2 item expired")
	}
}

// TestJoinValue_WriteRace_ConcurrentInsert_CoversLine107 runs many goroutines
// on the same key so that goroutines racing through the write lock find the
// item already present (the race-check path). We use 5 ms TTL instead of 1 ns
// to avoid the timer-fires-before-assignment startup race in MakeHashMapJoinStorage.
func TestJoinValue_WriteRace_ConcurrentInsert_CoversLine107(t *testing.T) {
	s, _ := makeStorage(t, 5*time.Millisecond, false)
	defer s.Stop(context.Background())

	var wg sync.WaitGroup
	const n = 200
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			s.JoinValue(context.Background(), "race", 0, 1, func(values [][]interface{}) bool {
				return false
			})
		}()
	}
	close(start)
	wg.Wait()
}

// ---------- context deadline tests ----------

// TestJoinValue_ExpiredContext_CallbackInvokedImmediately verifies that when the
// context deadline has already passed, f is called immediately regardless of the
// config TTL (the item gets a zero deadline, so IsZero() bypasses the deadline check).
func TestJoinValue_ExpiredContext_CallbackInvokedImmediately(t *testing.T) {
	s, _ := makeStorage(t, time.Hour, false)
	defer s.Stop(context.Background())

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	called := false
	s.JoinValue(ctx, "k1", 0, "val", func(values [][]interface{}) bool {
		called = true
		return true
	})

	if !called {
		t.Fatal("callback was not called with expired context")
	}
}

// TestJoinValue_ContextDeadline_UsedInsteadOfConfigTTL verifies that when the
// context carries a deadline, it overrides the config TTL: the item deadline is
// set to the context deadline, not to now+configTTL.
func TestJoinValue_ContextDeadline_UsedInsteadOfConfigTTL(t *testing.T) {
	s, _ := makeStorage(t, time.Hour, false)
	defer s.Stop(context.Background())

	ctxDeadline := time.Now().Add(time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), ctxDeadline)
	defer cancel()

	s.JoinValue(ctx, "k1", 0, "v", func(values [][]interface{}) bool { return false })

	s.lock.RLock()
	item := s.storage1["k1"]
	s.lock.RUnlock()

	if item == nil {
		t.Fatal("item not found in storage1")
	}
	// deadline must be close to ctxDeadline, not 1 hour from now
	if item.deadline.After(ctxDeadline.Add(time.Millisecond)) {
		t.Fatalf("item deadline %v is much later than context deadline %v — config TTL was not overridden", item.deadline, ctxDeadline)
	}
}

// TestJoinValue_Storage1ReadPath_CoverLine85 covers the case where an existing
// item with a future deadline is found in the read lock (lines 85-87). We
// pre-insert an item whose 1 ns deadline will have just expired by the time
// JoinValue's inner check runs, allowing the loop to complete naturally.
func TestJoinValue_Storage1ReadPath_CoverLine85(t *testing.T) {
	s, _ := makeStorage(t, time.Nanosecond, false)
	defer s.Stop(context.Background())

	// Pre-insert an item with a 1 ms deadline so the read lock finds it with a
	// future deadline (line 85-87) for many iterations before it expires.
	futureDeadline := time.Now().Add(time.Millisecond)
	s.rotateLock.RLock()
	s.lock.Lock()
	s.storage1["k_r85"] = &Item{
		values:   make([][]interface{}, 1),
		deadline: futureDeadline,
	}
	s.gaugeCount.Inc()
	s.lock.Unlock()
	s.rotateLock.RUnlock()

	// The goroutine below removes the item after the deadline so JoinValue can
	// exit the spin loop by creating a fresh 1 ns item.
	done := make(chan struct{})
	go func() {
		time.Sleep(2 * time.Millisecond) // let the item expire
		// JoinValue holds rotateLock.RLock() for the duration of the loop,
		// so rotate() cannot swap storage1. We only need lock.Lock() here.
		s.lock.Lock()
		if item, ok := s.storage1["k_r85"]; ok {
			item.lock.Lock()
			item.processed = true
			item.lock.Unlock()
			delete(s.storage1, "k_r85")
			s.gaugeCount.Dec()
		}
		s.lock.Unlock()
		close(done)
	}()

	called := false
	s.JoinValue(context.Background(), "k_r85", 0, "v", func(values [][]interface{}) bool {
		called = true
		return true
	})

	<-done
	if !called {
		t.Fatal("callback not called")
	}
}

// ---------- rotate tests ----------

func TestRotate_PreservesStorage1ItemsInStorage2(t *testing.T) {
	s, _ := makeStorage(t, time.Hour, false)
	defer s.Stop(context.Background())

	s.storage1["live"] = &Item{}

	// First rotate: "live" moves to storage2.
	s.rotate(context.Background())
	if _, ok := s.storage2["live"]; !ok {
		t.Fatal("item should be in storage2 after first rotate")
	}

	// Second rotate: storage2 (with "live") is evicted, storage1 (empty) becomes storage2.
	s.rotate(context.Background())
	if _, ok := s.storage2["live"]; ok {
		t.Fatal("item should be gone after second rotate")
	}
}
