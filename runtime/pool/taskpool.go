/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type Task struct {
	fn   func()
	next *Task
}

type TaskPool interface {
	Pool
	AddTask(ctx context.Context, fn func()) error
	GetName() string
	GetExecutorsCount() int
}

type TaskPoolImpl struct {
	head                *Task
	tail                *Task
	lock                sync.Mutex
	name                string
	gaugeQueueLength    metrics.Int64Gauge
	tasksTotal          metrics.Int64Counter
	executionDuration   metrics.Float64Histogram
	stopTimeoutCounter  metrics.Int64Counter
	taskRejectedCounter metrics.Int64Counter
	wg                  sync.WaitGroup
	done                bool
	cond                *sync.Cond
	count               int
	environment         environment.ServiceEnvironment
}

func makeTaskPool(env environment.ServiceEnvironment, poolConfig *config.PoolConfig) (TaskPool, error) {
	pool := &TaskPoolImpl{
		name:        poolConfig.Name,
		environment: env,
	}
	scope := env.Metrics().Scope("task_pool", metrics.Labels{
		"service": env.ServiceConfig().Name,
		"name":    poolConfig.Name,
	})
	var err error
	pool.gaugeQueueLength, err = scope.Gauge("queue_length", "Task pool wait queue length", nil)
	if err != nil {
		return nil, err
	}
	pool.tasksTotal, err = scope.Counter("tasks_total", "Total number of tasks executed by task pool", nil)
	if err != nil {
		return nil, err
	}
	pool.executionDuration, err = scope.Histogram("task_execution_duration_seconds", "Task execution duration in seconds", nil)
	if err != nil {
		return nil, err
	}
	pool.stopTimeoutCounter, err = scope.Counter("events_total", "Total number of events in task pool", metrics.Labels{"event": "stop_timeout"})
	if err != nil {
		return nil, err
	}
	pool.taskRejectedCounter, err = scope.Counter("events_total", "Total number of events in task pool", metrics.Labels{"event": "task_rejected"})
	if err != nil {
		return nil, err
	}
	pool.cond = sync.NewCond(&pool.lock)
	return pool, nil
}

func (p *TaskPoolImpl) GetName() string { return p.name }

func (p *TaskPoolImpl) GetExecutorsCount() int {
	return p.environment.RuntimeConfig().GetPoolByName(p.name).ExecutorsCount
}

func (p *TaskPoolImpl) AddTask(ctx context.Context, fn func()) error {
	if err := ctx.Err(); err != nil {
		p.taskRejectedCounter.Inc(ctx)
		return err
	}
	task := &Task{fn: fn}
	p.lock.Lock()
	if p.tail != nil {
		p.tail.next = task
	} else {
		p.head = task
	}
	p.tail = task
	p.count++
	p.lock.Unlock()
	p.cond.Signal()
	p.gaugeQueueLength.Inc()
	return nil
}

func (p *TaskPoolImpl) Start(ctx context.Context) error {
	poolConfig := p.environment.RuntimeConfig().GetPoolByName(p.name)
	executorsCount := poolConfig.ExecutorsCount
	if executorsCount == 0 {
		executorsCount = runtime.NumCPU()
	}
	for i := 0; i < executorsCount; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				p.lock.Lock()
				for p.count == 0 && !p.done {
					p.cond.Wait()
				}
				if p.count == 0 && p.done {
					p.lock.Unlock()
					break
				}
				task := p.head
				p.head = p.head.next
				if p.head == nil {
					p.tail = nil
				}
				task.next = nil
				p.count--
				p.lock.Unlock()
				p.gaugeQueueLength.Dec()
				startTime := time.Now()
				runTask(ctx, p.environment, p.name, task.fn)
				task.fn = nil
				p.tasksTotal.Inc(ctx)
				p.executionDuration.Observe(ctx, time.Since(startTime).Seconds())
			}
		}()
	}
	return nil
}

func (p *TaskPoolImpl) Stop(ctx context.Context) {
	p.lock.Lock()
	p.done = true
	p.cond.Broadcast()
	p.lock.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		p.lock.Lock()
		tasksCount := p.count
		p.lock.Unlock()
		p.environment.Log().Warnf(ctx, "task pool %q stopped by timeout: %s (tasks count=%d)", p.name, ctx.Err(), tasksCount)
		p.stopTimeoutCounter.Inc(ctx)
	}
}
