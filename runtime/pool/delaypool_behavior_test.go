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

func newTestDelayPool(t *testing.T) DelayPool {
	t.Helper()
	m := testmetrics.New()
	rc, err := config.NewRuntimeConfig(&minimalConfig{})
	require.NoError(t, err)
	pool, err := makeDelayPool(&mockPoolEnv{m: m, rc: rc})
	require.NoError(t, err)
	return pool
}

// TestDelayPool_ContextCancelRunsImmediately verifies that cancelling the task's
// context before the delay expires moves the task to the execute queue immediately
// instead of waiting for the scheduled delay.
func TestDelayPool_ContextCancelRunsImmediately(t *testing.T) {
	pool := newTestDelayPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop(context.Background())

	taskCtx, cancelTask := context.WithCancel(ctx)
	defer cancelTask()

	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, pool.Delay(taskCtx, 10*time.Second, func() {
		wg.Done()
	}))

	start := time.Now()
	cancelTask()
	wg.Wait()

	assert.Less(t, time.Since(start), time.Second,
		"task must run immediately on cancel, not after the 10s delay")
}
