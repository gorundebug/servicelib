/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import (
	"container/heap"
	"context"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

const defaultPriorityQueueCapacity = 256

type PriorityTask struct {
	fn            func()
	priority      int
	priorityIndex int
	stopFn        func() bool
}

type PriorityTaskPool interface {
	Pool
	AddTask(ctx context.Context, priority int, fn func()) error
	GetName() string
	GetExecutorsCount() int
}

type PriorityTaskPoolImpl struct {
	lock                sync.Mutex
	name                string
	pq                  *TaskPriorityQueue
	gaugeQueueLength    metrics.Int64Gauge
	tasksTotal          metrics.Int64Counter
	executionDuration   metrics.Float64Histogram
	stopTimeoutCounter  metrics.Int64Counter
	taskRejectedCounter metrics.Int64Counter
	taskExpiredCounter  metrics.Int64Counter
	wg                  sync.WaitGroup
	done                bool
	stop                chan struct{}
	stopOnce            sync.Once
	startOnce           sync.Once
	cond                *sync.Cond
	environment         environment.ServiceEnvironment
}

func makePriorityTaskPool(env environment.ServiceEnvironment, poolConfig *config.PoolConfig) (PriorityTaskPool, error) {
	capacity := poolConfig.QueueCapacity
	if capacity == 0 {
		capacity = defaultPriorityQueueCapacity
	}
	pq := make(TaskPriorityQueue, 0, capacity)
	pool := &PriorityTaskPoolImpl{
		name:        poolConfig.Name,
		pq:          &pq,
		environment: env,
		stop:        make(chan struct{}),
	}
	scope := env.Metrics().Scope("priority_task_pool", metrics.Labels{
		"service": env.ServiceConfig().Name,
		"name":    poolConfig.Name,
	})
	var err error
	pool.gaugeQueueLength, err = scope.Gauge("queue_length", "Priority task pool wait queue length", nil)
	if err != nil {
		return nil, err
	}
	pool.tasksTotal, err = scope.Counter("tasks_total", "Total number of tasks executed by priority task pool", nil)
	if err != nil {
		return nil, err
	}
	pool.executionDuration, err = scope.Histogram("task_execution_duration_seconds", "Task execution duration in seconds", nil)
	if err != nil {
		return nil, err
	}
	pool.stopTimeoutCounter, err = scope.Counter("events_total", "Total number of events in priority task pool", metrics.Labels{"event": "stop_timeout"})
	if err != nil {
		return nil, err
	}
	pool.taskRejectedCounter, err = scope.Counter("events_total", "Total number of events in priority task pool", metrics.Labels{"event": "task_rejected"})
	if err != nil {
		return nil, err
	}
	pool.taskExpiredCounter, err = scope.Counter("events_total", "Total number of events in priority task pool", metrics.Labels{"event": "task_expired"})
	if err != nil {
		return nil, err
	}
	pool.cond = sync.NewCond(&pool.lock)
	return pool, nil
}

// TaskPriorityQueue is a min-heap ordered by priority.
type TaskPriorityQueue []*PriorityTask

func (pq *TaskPriorityQueue) Len() int { return len(*pq) }

func (pq *TaskPriorityQueue) Less(i, j int) bool {
	return (*pq)[i].priority < (*pq)[j].priority
}

func (pq *TaskPriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
	(*pq)[i].priorityIndex = i
	(*pq)[j].priorityIndex = j
}

func (pq *TaskPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*PriorityTask)
	item.priorityIndex = n
	*pq = append(*pq, item)
}

func (pq *TaskPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.priorityIndex = -1
	*pq = old[0 : n-1]
	return item
}

func (p *PriorityTaskPoolImpl) GetName() string { return p.name }

func (p *PriorityTaskPoolImpl) GetExecutorsCount() int {
	return p.environment.RuntimeConfig().GetPoolByName(p.name).ExecutorsCount
}

func (p *PriorityTaskPoolImpl) AddTask(ctx context.Context, priority int, fn func()) error {
	if err := ctx.Err(); err != nil {
		p.taskRejectedCounter.Inc(ctx)
		return err
	}
	task := &PriorityTask{
		fn:            fn,
		priority:      priority,
		priorityIndex: -1,
	}
	p.lock.Lock()
	if p.done {
		p.lock.Unlock()
		p.taskRejectedCounter.Inc(ctx)
		return ErrPoolStopped
	}
	heap.Push(p.pq, task)
	task.stopFn = context.AfterFunc(ctx, func() {
		p.lock.Lock()
		if task.priorityIndex < 0 {
			p.lock.Unlock()
			return
		}
		task.priority = math.MinInt
		heap.Fix(p.pq, task.priorityIndex)
		p.lock.Unlock()
		p.cond.Signal()
		p.taskExpiredCounter.Inc(ctx)
	})
	p.lock.Unlock()
	p.cond.Signal()
	p.gaugeQueueLength.Inc()
	return nil
}

func (p *PriorityTaskPoolImpl) Start(ctx context.Context) error {
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

				p.lock.Lock()

				select {
				case <-p.stop:
					p.lock.Unlock()
					return
				default:
				}

				for i := 0; i < executorsCount; i++ {
					p.wg.Add(1)

					go func(restart *bool) {
						defer p.wg.Done()
						for {
							p.lock.Lock()
							if *restart {
								p.lock.Unlock()
								p.cond.Signal()
								break
							}
							for p.pq.Len() == 0 && !p.done {
								p.cond.Wait()
							}
							if p.pq.Len() == 0 && p.done {
								p.lock.Unlock()
								break
							}
							task := heap.Pop(p.pq).(*PriorityTask)
							stopFn := task.stopFn
							p.lock.Unlock()
							stopFn()
							p.gaugeQueueLength.Dec()
							startTime := time.Now()
							runTask(ctx, p.environment, p.name, task.fn)
							task.fn = nil
							p.tasksTotal.Inc(ctx)
							p.executionDuration.Observe(ctx, time.Since(startTime).Seconds())
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
		<-started
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

func (p *PriorityTaskPoolImpl) Stop(ctx context.Context) {
	p.stopOnce.Do(func() {
		p.lock.Lock()
		close(p.stop)
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
		tasksCount := p.pq.Len()
		p.lock.Unlock()
		p.environment.Log().Warn(ctx, "priority task pool stopped by timeout", log.Str("pool", p.name), log.Err(ctx.Err()), log.Int("tasks_count", tasksCount))
		p.stopTimeoutCounter.Inc(ctx)
		p.wg.Wait()
	})
}
