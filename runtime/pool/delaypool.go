/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type DelayPool interface {
	Pool
	Delay(ctx context.Context, deadline time.Duration, fn func()) error
}

// delayTask holds all per-task state. Using a struct with methods instead of
// closures lets the compiler represent method values as (static funcptr, *task)
// without allocating a separate closure environment object for each callback.
//
// Ordering invariant: timer is set before context.AfterFunc, so onCtxDone
// always sees a valid timer. normalRun and stopCtxFn form a two-sided atomic
// handshake so a timer that wins before registration completes still removes
// the context callback. Tasks with deadline <= 0 skip that handshake and run
// directly via a goroutine.
type delayTask struct {
	once             sync.Once
	timer            *time.Timer
	stopCtxFn        atomic.Pointer[func() bool]
	normalRun        atomic.Bool
	usesCtxAfterFunc bool
	fn               func()
	p                *DelayPoolImpl
	ctx              context.Context
}

func (t *delayTask) onTimer() {
	t.once.Do(t.runNormal)
}

func (t *delayTask) runNormal() {
	if t.usesCtxAfterFunc {
		t.normalRun.Store(true)
		// Release the context.AfterFunc registration immediately so the task
		// struct is not kept alive until ctx is cancelled.
		if p := t.stopCtxFn.Load(); p != nil {
			(*p)()
		}
	}
	defer t.p.wg.Done()
	defer t.p.gaugeWaitQueueLength.Dec()
	startTime := time.Now()
	runTask(t.ctx, t.p.environment, "delay", t.fn)
	t.fn = nil
	t.p.tasksTotal.Inc(t.ctx)
	t.p.executionDuration.Observe(t.ctx, time.Since(startTime).Seconds())
}

func (t *delayTask) onCtxDone() {
	t.timer.Stop()
	t.once.Do(t.runCancelled)
}

func (t *delayTask) runCancelled() {
	defer t.p.wg.Done()
	defer t.p.gaugeWaitQueueLength.Dec()
	startTime := time.Now()
	runTask(t.ctx, t.p.environment, "delay", t.fn)
	t.fn = nil
	t.p.tasksTotal.Inc(t.ctx)
	t.p.executionDuration.Observe(t.ctx, time.Since(startTime).Seconds())
	t.p.taskCancelledCounter.Inc(t.ctx)
}

// DelayPoolImpl schedules tasks via per-task time.AfterFunc timers.
// context.AfterFunc handles early cancellation; sync.Once per task guarantees fn
// is called exactly once regardless of which path (timer or ctx cancel) wins.
//
// Stop semantics: Stop marks the pool as done (no new tasks accepted) and waits
// for all pending tasks to complete, bounded by the Stop ctx deadline. Pending
// timers are NOT cancelled — tasks with large deadlines will run at their
// scheduled time unless the Stop ctx expires first.
type DelayPoolImpl struct {
	wg        sync.WaitGroup
	mu        sync.Mutex
	done      bool
	stopOnce  sync.Once
	startOnce sync.Once

	gaugeWaitQueueLength metrics.Int64Gauge
	tasksTotal           metrics.Int64Counter
	executionDuration    metrics.Float64Histogram
	stopTimeoutCounter   metrics.Int64Counter
	taskCancelledCounter metrics.Int64Counter
	environment          environment.ServiceEnvironment
}

func makeDelayPool(env environment.ServiceEnvironment) (DelayPool, error) {
	pool := &DelayPoolImpl{environment: env}
	scope := env.Metrics().Scope("delay_pool", metrics.Labels{
		"service": env.ServiceConfig().Name,
	})
	var err error
	pool.gaugeWaitQueueLength, err = scope.Gauge("wait_queue_length", "Delay pool wait queue length", nil)
	if err != nil {
		return nil, err
	}
	pool.tasksTotal, err = scope.Counter("tasks_total", "Total number of tasks executed by delay pool", nil)
	if err != nil {
		return nil, err
	}
	pool.executionDuration, err = scope.Histogram("task_execution_duration_seconds", "Task execution duration in seconds", nil)
	if err != nil {
		return nil, err
	}
	pool.stopTimeoutCounter, err = scope.Counter("events_total", "Total number of events in delay pool", metrics.Labels{"event": "stop_timeout"})
	if err != nil {
		return nil, err
	}
	pool.taskCancelledCounter, err = scope.Counter("events_total", "Total number of events in delay pool", metrics.Labels{"event": "task_cancelled"})
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func (p *DelayPoolImpl) Delay(ctx context.Context, deadline time.Duration, fn func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ctxDeadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(ctxDeadline); remaining < deadline {
			deadline = remaining
		}
	}

	p.mu.Lock()
	if p.done {
		p.mu.Unlock()
		return ErrPoolStopped
	}
	p.wg.Add(1)
	p.mu.Unlock()

	p.gaugeWaitQueueLength.Inc()

	// Fast path: run immediately without timer or context registration.
	// Also covers the case where ctx deadline has already passed (remaining <= 0).
	if deadline <= 0 {
		task := &delayTask{fn: fn, p: p, ctx: ctx}
		go task.runNormal()
		return nil
	}

	task := &delayTask{fn: fn, p: p, ctx: ctx, usesCtxAfterFunc: true}
	// timer set before context.AfterFunc: onCtxDone always sees a valid timer.
	task.timer = time.AfterFunc(deadline, task.onTimer)
	stop := context.AfterFunc(ctx, task.onCtxDone)
	task.stopCtxFn.Store(&stop)
	// The timer may have completed before context.AfterFunc returned and before
	// stopCtxFn became visible to runNormal. Complete the other half of the
	// handshake so every interleaving unregisters the callback.
	if task.normalRun.Load() {
		stop()
	}

	return nil
}

func (p *DelayPoolImpl) Start(_ context.Context) error {
	var called bool
	p.startOnce.Do(func() {
		p.mu.Lock()
		isDone := p.done
		p.mu.Unlock()
		if isDone {
			return
		}
		called = true
	})
	if !called {
		p.mu.Lock()
		isDone := p.done
		p.mu.Unlock()
		if isDone {
			return ErrPoolStopped
		}
		return ErrPoolAlreadyStarted
	}
	return nil
}

func (p *DelayPoolImpl) Stop(ctx context.Context) {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.done = true
		p.mu.Unlock()

		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			p.environment.Log().Warn(ctx, "delay pool stopped by timeout", log.Err(ctx.Err()))
			p.stopTimeoutCounter.Inc(ctx)
		}
	})
}
