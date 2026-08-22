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
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type blockingAfterFuncContext struct {
	done                chan struct{}
	registrationStarted chan struct{}
	allowRegistration   chan struct{}
	stopCalled          chan struct{}
	registeredOnce      sync.Once
	stopOnce            sync.Once
	stopped             atomic.Bool
}

func newBlockingAfterFuncContext() *blockingAfterFuncContext {
	return &blockingAfterFuncContext{
		done:                make(chan struct{}),
		registrationStarted: make(chan struct{}),
		allowRegistration:   make(chan struct{}),
		stopCalled:          make(chan struct{}),
	}
}

func (c *blockingAfterFuncContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *blockingAfterFuncContext) Done() <-chan struct{}       { return c.done }
func (c *blockingAfterFuncContext) Err() error                  { return nil }
func (c *blockingAfterFuncContext) Value(any) any               { return nil }

func (c *blockingAfterFuncContext) AfterFunc(func()) func() bool {
	c.registeredOnce.Do(func() { close(c.registrationStarted) })
	<-c.allowRegistration
	return func() bool {
		if !c.stopped.CompareAndSwap(false, true) {
			return false
		}
		c.stopOnce.Do(func() { close(c.stopCalled) })
		return true
	}
}

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

func TestDelayPool_TimerCompletionUnregistersLateContextAfterFunc(t *testing.T) {
	pool := newTestDelayPool(t)
	require.NoError(t, pool.Start(context.Background()))
	defer pool.Stop(context.Background())

	ctx := newBlockingAfterFuncContext()
	taskRan := make(chan struct{})
	delayReturned := make(chan error, 1)
	go func() {
		delayReturned <- pool.Delay(ctx, time.Millisecond, func() { close(taskRan) })
	}()

	<-ctx.registrationStarted
	select {
	case <-taskRan:
	case <-time.After(time.Second):
		t.Fatal("timer did not complete while context.AfterFunc registration was blocked")
	}
	close(ctx.allowRegistration)
	require.NoError(t, <-delayReturned)

	select {
	case <-ctx.stopCalled:
	case <-time.After(time.Second):
		t.Fatal("completed timer left context.AfterFunc registered")
	}
}

func TestDelayPool_StopTimeoutReportsButStillDrainsAcceptedTask(t *testing.T) {
	pool := newTestDelayPool(t)
	require.NoError(t, pool.Start(context.Background()))

	started := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan struct{})
	require.NoError(t, pool.Delay(context.Background(), 0, func() {
		close(started)
		<-release
		close(completed)
	}))
	<-started

	stopContext, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		pool.Stop(stopContext)
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("delay pool detached an accepted callback at the stop deadline")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("accepted callback did not complete")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("delay pool did not finish after the callback drained")
	}
}
