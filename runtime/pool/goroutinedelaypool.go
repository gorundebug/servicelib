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
	"time"

	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type GoroutineDelayPoolImpl struct {
	wg        sync.WaitGroup
	stopOnce  sync.Once
	startOnce sync.Once
	mu        sync.Mutex
	done      bool

	gaugeActiveCount   metrics.Int64Gauge
	tasksTotal         metrics.Int64Counter
	executionDuration  metrics.Float64Histogram
	stopTimeoutCounter metrics.Int64Counter
	environment        environment.ServiceEnvironment
}

func makeGoroutineDelayPool(env environment.ServiceEnvironment) (DelayPool, error) {
	pool := &GoroutineDelayPoolImpl{
		environment: env,
	}
	scope := env.Metrics().Scope("goroutine_delay_pool", metrics.Labels{
		"service": env.ServiceConfig().Name,
	})
	var err error
	pool.gaugeActiveCount, err = scope.Gauge("active_count", "Goroutine delay pool active goroutines", nil)
	if err != nil {
		return nil, err
	}
	pool.tasksTotal, err = scope.Counter("tasks_total", "Total number of tasks executed by goroutine delay pool", nil)
	if err != nil {
		return nil, err
	}
	pool.executionDuration, err = scope.Histogram("task_execution_duration_seconds", "Task execution duration in seconds", nil)
	if err != nil {
		return nil, err
	}
	pool.stopTimeoutCounter, err = scope.Counter("events_total", "Total number of events in goroutine delay pool", metrics.Labels{"event": "stop_timeout"})
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func (p *GoroutineDelayPoolImpl) Delay(ctx context.Context, deadline time.Duration, fn func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	if p.done {
		p.mu.Unlock()
		return ErrPoolStopped
	}
	p.wg.Add(1)
	p.mu.Unlock()

	p.gaugeActiveCount.Inc()

	go func() {
		defer p.wg.Done()
		defer p.gaugeActiveCount.Dec()

		timer := time.NewTimer(deadline)
		defer timer.Stop()

		select {
		case <-timer.C:
			startTime := time.Now()
			runTask(ctx, p.environment, "goroutine_delay", fn)
			p.tasksTotal.Inc(ctx)
			p.executionDuration.Observe(ctx, time.Since(startTime).Seconds())
		case <-ctx.Done():
			// ctx cancelled: fn must still be called so callers can close spans
			// and release resources. fn checks ctx.Err() and exits early.
			runTask(ctx, p.environment, "goroutine_delay", fn)
		}
	}()

	return nil
}

func (p *GoroutineDelayPoolImpl) Start(_ context.Context) error {
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

func (p *GoroutineDelayPoolImpl) Stop(ctx context.Context) {
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
			p.environment.Log().Warn(ctx, "goroutine delay pool stopped by timeout", log.Err(ctx.Err()))
			p.stopTimeoutCounter.Inc(ctx)
		}
	})
}
