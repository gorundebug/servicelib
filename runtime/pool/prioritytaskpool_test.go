/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	envlog "github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimalConfig is a stub config.Config that only registers pool configs.
type minimalConfig struct {
	pools []*config.PoolConfig
}

func (c *minimalConfig) GetServices() []*config.ServiceConfig            { return nil }
func (c *minimalConfig) GetStreams() []config.StreamConfig               { return nil }
func (c *minimalConfig) GetDataConnectors() []config.DataConnectorConfig { return nil }
func (c *minimalConfig) GetEndpoints() []config.EndpointConfig           { return nil }
func (c *minimalConfig) GetPools() []*config.PoolConfig                  { return c.pools }
func (c *minimalConfig) GetLinks() []*config.LinkConfig                  { return nil }
func (c *minimalConfig) GetModules() []*config.ModuleConfig              { return nil }
func (c *minimalConfig) GetTypes() []*config.TypeConfig                  { return nil }
func (c *minimalConfig) GetProperty(_ string) interface{}                { return nil }
func (c *minimalConfig) ApplyEnvironment() error                         { return nil }

type noopLogger struct{}

func (noopLogger) Debug(_ context.Context, _ string, _ ...envlog.Field) {}
func (noopLogger) Info(_ context.Context, _ string, _ ...envlog.Field)  {}
func (noopLogger) Warn(_ context.Context, _ string, _ ...envlog.Field)  {}
func (noopLogger) Error(_ context.Context, _ string, _ ...envlog.Field) {}

type mockPoolEnv struct {
	environment.ServiceEnvironment
	m  *testmetrics.TestMetrics
	rc *config.RuntimeConfig
}

func (e *mockPoolEnv) Metrics() metrics.Metrics { return e.m }
func (e *mockPoolEnv) ServiceConfig() *config.ServiceConfig {
	return &config.ServiceConfig{Name: "test-svc"}
}
func (e *mockPoolEnv) RuntimeConfig() *config.RuntimeConfig { return e.rc }
func (e *mockPoolEnv) Log() envlog.Logger                   { return noopLogger{} }

func newTestPriorityPool(t *testing.T, name string, executors int) PriorityTaskPool {
	pool, _ := newTestPriorityPoolWithMetrics(t, name, executors)
	return pool
}

func newTestPriorityPoolWithMetrics(t *testing.T, name string, executors int) (PriorityTaskPool, *testmetrics.TestMetrics) {
	t.Helper()
	m := testmetrics.New()
	poolCfg := &config.PoolConfig{Name: name, ExecutorsCount: executors}
	rc, err := config.NewRuntimeConfig(&minimalConfig{pools: []*config.PoolConfig{poolCfg}})
	require.NoError(t, err)
	pool, err := makePriorityTaskPool(&mockPoolEnv{m: m, rc: rc}, poolCfg)
	require.NoError(t, err)
	return pool, m
}

func TestPriorityTaskPool_ExecutorMetrics(t *testing.T) {
	pool, m := newTestPriorityPoolWithMetrics(t, "priority-metrics", 1)
	ctx := context.Background()
	require.NoError(t, pool.Start(ctx))

	started := make(chan struct{})
	release := make(chan struct{})
	require.NoError(t, pool.AddTask(ctx, 0, func() {
		close(started)
		<-release
	}))
	<-started

	labels := metrics.Labels{"service": "test-svc", "name": "priority-metrics"}
	assert.Equal(t, int64(1), m.Gauge("priority_task_pool_executors_target", labels).Value())
	assert.Equal(t, int64(1), m.Gauge("priority_task_pool_executors_allocated", labels).Value())
	assert.Equal(t, int64(1), m.Gauge("priority_task_pool_executors_busy", labels).Value())

	close(release)
	pool.Stop(ctx)
	assert.Equal(t, int64(0), m.Gauge("priority_task_pool_executors_allocated", labels).Value())
	assert.Equal(t, int64(0), m.Gauge("priority_task_pool_executors_busy", labels).Value())
}

func TestPriorityTaskPool_UsesConstructorConfigWhenRuntimePoolIsMissing(t *testing.T) {
	m := testmetrics.New()
	rc, err := config.NewRuntimeConfig(&minimalConfig{})
	require.NoError(t, err)
	poolConfig := &config.PoolConfig{Name: "fallback-priority", ExecutorsCount: 1}
	priorityPool, err := makePriorityTaskPool(&mockPoolEnv{m: m, rc: rc}, poolConfig)
	require.NoError(t, err)
	require.Equal(t, 1, priorityPool.GetExecutorsCount())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, priorityPool.Start(ctx))
	executed := make(chan struct{})
	require.NoError(t, priorityPool.AddTask(ctx, 10, func() { close(executed) }))
	select {
	case <-executed:
	case <-ctx.Done():
		t.Fatal("fallback priority pool did not execute the task")
	}
	priorityPool.Stop(context.Background())
}

// TestPriorityTaskPool_PriorityOrdering verifies that queued tasks execute in
// ascending priority order (lower value = higher priority).
func TestPriorityTaskPool_PriorityOrdering(t *testing.T) {
	pool := newTestPriorityPool(t, "ordering", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(context.Background())

	block := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, pool.AddTask(ctx, 0, func() {
		defer wg.Done()
		<-block
	}))
	// Give the executor time to pick up the blocker before we enqueue the rest.
	time.Sleep(10 * time.Millisecond)

	var mu sync.Mutex
	var order []int
	for _, p := range []int{5, 3, 1, 4, 2} {
		p := p
		wg.Add(1)
		require.NoError(t, pool.AddTask(ctx, p, func() {
			defer wg.Done()
			mu.Lock()
			order = append(order, p)
			mu.Unlock()
		}))
	}

	close(block)
	wg.Wait()

	assert.Equal(t, []int{1, 2, 3, 4, 5}, order)
}

// TestPriorityTaskPool_CancelPromotion verifies that a low-priority task whose
// context is cancelled (via explicit cancel or deadline) is promoted to the front
// of the queue via context.AfterFunc and executes before a higher-priority task.
func TestPriorityTaskPool_CancelPromotion(t *testing.T) {
	pool := newTestPriorityPool(t, "promotion", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(context.Background())

	block := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, pool.AddTask(ctx, 0, func() {
		defer wg.Done()
		<-block
	}))
	time.Sleep(10 * time.Millisecond)

	var mu sync.Mutex
	var order []string

	lowCtx, cancelLow := context.WithCancel(ctx)
	defer cancelLow()
	wg.Add(1)
	require.NoError(t, pool.AddTask(lowCtx, 100, func() {
		defer wg.Done()
		mu.Lock()
		order = append(order, "low")
		mu.Unlock()
	}))

	wg.Add(1)
	require.NoError(t, pool.AddTask(ctx, 1, func() {
		defer wg.Done()
		mu.Lock()
		order = append(order, "high")
		mu.Unlock()
	}))

	// Cancel the low-priority context — AfterFunc fires and promotes the task immediately.
	cancelLow()
	time.Sleep(10 * time.Millisecond)

	close(block)
	wg.Wait()

	assert.Equal(t, []string{"low", "high"}, order)
}
