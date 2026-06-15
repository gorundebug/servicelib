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
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTaskPool(t *testing.T, name string, executors int) TaskPool {
	t.Helper()
	m := testmetrics.New()
	poolCfg := &config.PoolConfig{Name: name, ExecutorsCount: executors}
	rc, err := config.NewRuntimeConfig(&minimalConfig{pools: []*config.PoolConfig{poolCfg}})
	require.NoError(t, err)
	pool, err := makeTaskPool(&mockPoolEnv{m: m, rc: rc}, poolCfg)
	require.NoError(t, err)
	return pool
}

// TestTaskPool_CancelMovesToFront verifies that cancelling a task's context
// moves it to the head of the queue so it runs before tasks added earlier,
// allowing resources held in the pipeline to be released as fast as possible.
func TestTaskPool_CancelMovesToFront(t *testing.T) {
	pool := newTestTaskPool(t, "cancel", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(context.Background())

	block := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, pool.AddTask(ctx, func() {
		defer wg.Done()
		<-block
	}))
	time.Sleep(10 * time.Millisecond)

	var mu sync.Mutex
	var order []string

	// Two normal tasks sitting in the queue.
	for _, name := range []string{"first", "second"} {
		name := name
		wg.Add(1)
		require.NoError(t, pool.AddTask(ctx, func() {
			defer wg.Done()
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
		}))
	}

	// Task added last but with a context that will be cancelled.
	cancelCtx, cancelTask := context.WithCancel(ctx)
	defer cancelTask()
	wg.Add(1)
	require.NoError(t, pool.AddTask(cancelCtx, func() {
		defer wg.Done()
		mu.Lock()
		order = append(order, "cancelled")
		mu.Unlock()
	}))

	// Cancel — AfterFunc moves the task to the head of the queue.
	cancelTask()
	time.Sleep(10 * time.Millisecond)

	close(block)
	wg.Wait()

	assert.Equal(t, []string{"cancelled", "first", "second"}, order)
}
