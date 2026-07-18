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
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type Task struct {
	fn      func()
	next    *Task
	prev    *Task
	inQueue bool
	stopFn  func() bool
}

type TaskPool interface {
	Pool
	AddTask(ctx context.Context, fn func()) error
	GetName() string
	GetExecutorsCount() int
}

type TaskPoolImpl struct {
	head                    *Task
	tail                    *Task
	lock                    sync.Mutex
	name                    string
	gaugeQueueLength        metrics.Int64Gauge
	gaugeExecutorsTarget    metrics.Int64Gauge
	gaugeExecutorsAllocated metrics.Int64Gauge
	gaugeExecutorsBusy      metrics.Int64Gauge
	tasksTotal              metrics.Int64Counter
	executionDuration       metrics.Float64Histogram
	stopTimeoutCounter      metrics.Int64Counter
	taskRejectedCounter     metrics.Int64Counter
	taskCancelledCounter    metrics.Int64Counter
	wg                      sync.WaitGroup
	done                    bool
	stop                    chan struct{}
	stopOnce                sync.Once
	startOnce               sync.Once
	cond                    *sync.Cond
	count                   int
	environment             environment.ServiceEnvironment
}

func makeTaskPool(env environment.ServiceEnvironment, poolConfig *config.PoolConfig) (TaskPool, error) {
	pool := &TaskPoolImpl{
		name:        poolConfig.Name,
		environment: env,
		stop:        make(chan struct{}),
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
	pool.gaugeExecutorsTarget, err = scope.Gauge("executors_target", "Desired number of task pool executors", nil)
	if err != nil {
		return nil, err
	}
	pool.gaugeExecutorsAllocated, err = scope.Gauge("executors_allocated", "Number of live task pool executors", nil)
	if err != nil {
		return nil, err
	}
	pool.gaugeExecutorsBusy, err = scope.Gauge("executors_busy", "Number of task pool executors running callbacks", nil)
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
	pool.taskCancelledCounter, err = scope.Counter("events_total", "Total number of events in task pool", metrics.Labels{"event": "task_cancelled"})
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
	if p.done {
		p.lock.Unlock()
		p.taskRejectedCounter.Inc(ctx)
		return ErrPoolStopped
	}
	if p.tail != nil {
		p.tail.next = task
		task.prev = p.tail
	} else {
		p.head = task
	}
	p.tail = task
	p.count++
	task.inQueue = true
	task.stopFn = context.AfterFunc(ctx, func() {
		p.lock.Lock()
		if !task.inQueue || p.head == task {
			p.lock.Unlock()
			return
		}
		// Remove from current position.
		task.prev.next = task.next
		if task.next != nil {
			task.next.prev = task.prev
		} else {
			p.tail = task.prev
		}
		// Insert at head.
		task.next = p.head
		task.prev = nil
		p.head.prev = task
		p.head = task
		p.lock.Unlock()
		p.cond.Signal()
		p.taskCancelledCounter.Inc(ctx)
	})
	p.lock.Unlock()
	p.cond.Signal()
	p.gaugeQueueLength.Inc()
	return nil
}

func (p *TaskPoolImpl) Start(ctx context.Context) error {
	var called bool
	p.startOnce.Do(func() {
		p.lock.Lock()
		isDone := p.done
		p.lock.Unlock()
		if isDone {
			return
		}
		called = true
		started := make(chan struct{})
		startedCh := started // captured by outer goroutine; never written by inner goroutine
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			executorsCount := 0
			var pRestart *bool
			for {
				poolConfig := p.environment.RuntimeConfig().GetPoolByName(p.name)
				executorsCountNew := poolConfig.ExecutorsCount
				if executorsCountNew == 0 {
					executorsCountNew = runtime.NumCPU()
				}
				if executorsCount == executorsCountNew {
					select {
					case <-ctx.Done():
						return
					case <-p.stop:
						return
					case <-ticker.C:
						continue
					}
				}
				if pRestart != nil {
					p.lock.Lock()
					*pRestart = true
					p.cond.Broadcast()
					p.lock.Unlock()
				}
				pRestart = new(bool)
				executorsCount = executorsCountNew
				p.gaugeExecutorsTarget.Set(int64(executorsCount))

				p.lock.Lock()
				select {
				case <-p.stop:
					p.lock.Unlock()
					return
				default:
				}
				for i := 0; i < executorsCount; i++ {
					p.wg.Add(1)
					p.gaugeExecutorsAllocated.Inc()
					go func(restart *bool) {
						defer p.wg.Done()
						defer p.gaugeExecutorsAllocated.Dec()
						for {
							p.lock.Lock()
							if *restart {
								p.lock.Unlock()
								p.cond.Signal()
								break
							}
							for p.count == 0 && !p.done {
								p.cond.Wait()
							}
							if p.count == 0 && p.done {
								p.lock.Unlock()
								break
							}
							task := p.head
							p.head = task.next
							if p.head != nil {
								p.head.prev = nil
							} else {
								p.tail = nil
							}
							task.next = nil
							task.inQueue = false
							stopFn := task.stopFn
							p.count--
							p.lock.Unlock()
							stopFn()
							p.gaugeQueueLength.Dec()
							func() {
								p.gaugeExecutorsBusy.Inc()
								defer p.gaugeExecutorsBusy.Dec()
								startTime := time.Now()
								runTask(ctx, p.environment, p.name, task.fn)
								task.fn = nil
								p.tasksTotal.Inc(ctx)
								p.executionDuration.Observe(ctx, time.Since(startTime).Seconds())
							}()
						}
					}(pRestart)
				}
				p.lock.Unlock()
				if started != nil {
					close(started)
					started = nil
				}
			}
		}()
		<-startedCh
	})
	if !called {
		p.lock.Lock()
		isDone := p.done
		p.lock.Unlock()
		if isDone {
			return ErrPoolStopped
		}
		return ErrPoolAlreadyStarted
	}
	return nil
}

func (p *TaskPoolImpl) Stop(ctx context.Context) {
	p.stopOnce.Do(func() {
		close(p.stop)

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
			return
		case <-ctx.Done():
		}
		p.lock.Lock()
		tasksCount := p.count
		p.lock.Unlock()
		p.environment.Log().Warn(ctx, "task pool stopped by timeout", log.Str("pool", p.name), log.Err(ctx.Err()), log.Int("tasks_count", tasksCount))
		p.stopTimeoutCounter.Inc(ctx)
		p.wg.Wait()
	})
}
