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
	"runtime"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type DelayPool interface {
	Pool
	Delay(ctx context.Context, deadline time.Duration, fn func()) error
}

type DelayTask struct {
	deadline time.Time
	fn       func()
	index    int
	next     *DelayTask
}

type DelayTaskPriorityQueue []*DelayTask

func (pq *DelayTaskPriorityQueue) Len() int { return len(*pq) }

func (pq *DelayTaskPriorityQueue) Less(i, j int) bool {
	return (*pq)[i].deadline.Before((*pq)[j].deadline)
}

func (pq *DelayTaskPriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
	(*pq)[i].index = i
	(*pq)[j].index = j
}

func (pq *DelayTaskPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*DelayTask)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *DelayTaskPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

type DelayPoolImpl struct {
	pq                      *DelayTaskPriorityQueue
	wg                      sync.WaitGroup
	timer                   *time.Timer
	lock                    sync.Mutex
	stopCh                  chan struct{}
	workerStop              chan struct{}
	cond                    *sync.Cond
	tasksLock               sync.Mutex
	done                    bool
	stop                    bool
	stopOnce                sync.Once
	startOnce               sync.Once
	gaugeWaitQueueLength    metrics.Int64Gauge
	gaugeExecuteQueueLength metrics.Int64Gauge
	tasksTotal              metrics.Int64Counter
	executionDuration       metrics.Float64Histogram
	stopTimeoutCounter      metrics.Int64Counter
	head                    *DelayTask
	tail                    *DelayTask
	count                   int
	environment             environment.ServiceEnvironment
}

func makeDelayPool(env environment.ServiceEnvironment) (DelayPool, error) {
	pool := &DelayPoolImpl{
		pq:          &DelayTaskPriorityQueue{},
		environment: env,
		workerStop:  make(chan struct{}),
	}
	scope := env.Metrics().Scope("delay_pool", metrics.Labels{
		"service": env.ServiceConfig().Name,
	})
	var err error
	pool.gaugeWaitQueueLength, err = scope.Gauge("wait_queue_length", "Delay pool wait queue length", nil)
	if err != nil {
		return nil, err
	}
	pool.gaugeExecuteQueueLength, err = scope.Gauge("execute_queue_length", "Delay pool execute queue length", nil)
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
	pool.cond = sync.NewCond(&pool.tasksLock)
	return pool, nil
}

func (p *DelayPoolImpl) processTimer() {
	p.lock.Lock()
	defer p.lock.Unlock()
	for p.pq.Len() > 0 && !(*p.pq)[0].deadline.After(time.Now()) {
		task := heap.Pop(p.pq).(*DelayTask)
		p.tasksLock.Lock()
		if p.tail != nil {
			p.tail.next = task
		} else {
			p.head = task
		}
		p.tail = task
		p.count++
		p.gaugeExecuteQueueLength.Inc()
		p.cond.Signal()
		p.tasksLock.Unlock()
		p.gaugeWaitQueueLength.Dec()
	}
	if p.pq.Len() > 0 {
		p.timer.Reset(time.Until((*p.pq)[0].deadline))
	} else if p.stopCh != nil && !p.stop {
		p.stop = true
		close(p.stopCh)
	}
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
	task := &DelayTask{
		fn:    fn,
		index: -1,
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	p.tasksLock.Lock()
	isDone := p.done
	p.tasksLock.Unlock()
	if isDone {
		return ErrPoolStopped
	}
	task.deadline = time.Now().Add(deadline)
	if p.pq.Len() == 0 || task.deadline.Before((*p.pq)[0].deadline) {
		if p.timer != nil {
			p.timer.Reset(deadline)
		} else {
			p.timer = time.AfterFunc(deadline, p.processTimer)
		}
	}
	heap.Push(p.pq, task)
	p.gaugeWaitQueueLength.Inc()
	return nil
}

func (p *DelayPoolImpl) Start(ctx context.Context) error {
	var called bool
	p.startOnce.Do(func() {
		p.tasksLock.Lock()
		isDone := p.done
		p.tasksLock.Unlock()
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
				executorsCountNew := p.environment.ServiceConfig().DelayExecutors
				if executorsCountNew == 0 {
					executorsCountNew = runtime.NumCPU()
				}
				if executorsCount == executorsCountNew {
					select {
					case <-ctx.Done():
						return
					case <-p.workerStop:
						return
					case <-ticker.C:
						continue
					}
				}
				if pRestart != nil {
					p.tasksLock.Lock()
					*pRestart = true
					p.cond.Broadcast()
					p.tasksLock.Unlock()
				}
				pRestart = new(bool)
				executorsCount = executorsCountNew

				p.tasksLock.Lock()
				select {
				case <-p.workerStop:
					p.tasksLock.Unlock()
					return
				default:
				}
				for i := 0; i < executorsCount; i++ {
					p.wg.Add(1)
					go func(restart *bool) {
						defer p.wg.Done()
						for {
							p.tasksLock.Lock()
							if *restart {
								p.tasksLock.Unlock()
								p.cond.Signal()
								break
							}
							for p.count == 0 && !p.done {
								p.cond.Wait()
							}
							if p.count == 0 && p.done {
								p.tasksLock.Unlock()
								break
							}
							task := p.head
							p.head = p.head.next
							if p.head == nil {
								p.tail = nil
							}
							task.next = nil
							p.count--
							p.gaugeExecuteQueueLength.Dec()
							p.tasksLock.Unlock()
							startTime := time.Now()
							runTask(ctx, p.environment, "delay", task.fn)
							task.fn = nil
							p.tasksTotal.Inc(ctx)
							p.executionDuration.Observe(ctx, time.Since(startTime).Seconds())
						}
					}(pRestart)
				}
				p.tasksLock.Unlock()
				if started != nil {
					close(started)
					started = nil
				}
			}
		}()
		<-started
	})
	if !called {
		p.tasksLock.Lock()
		isDone := p.done
		p.tasksLock.Unlock()
		if isDone {
			return ErrPoolStopped
		}
		return ErrPoolAlreadyStarted
	}
	return nil
}

func (p *DelayPoolImpl) Stop(ctx context.Context) {
	p.stopOnce.Do(func() {
		p.lock.Lock()
		if p.pq.Len() > 0 {
			go func() {
				p.stopCh = make(chan struct{})
				p.lock.Unlock()
				select {
				case <-p.stopCh:
				case <-ctx.Done():
					p.lock.Lock()
					p.environment.Log().Warn(ctx, "delay task pool stopped by timeout with waiting tasks",
						log.Int("waiting_count", p.pq.Len()), log.Err(ctx.Err()))
					p.lock.Unlock()
					p.stopTimeoutCounter.Inc(ctx)
				}
			}()
		} else {
			p.lock.Unlock()
		}
		p.lock.Lock()
		if p.pq.Len() == 0 {
			p.lock.Unlock()
			close(p.workerStop)
			p.tasksLock.Lock()
			p.done = true
			p.cond.Broadcast()
			p.tasksLock.Unlock()
			done := make(chan struct{})
			go func() {
				p.wg.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-ctx.Done():
				p.tasksLock.Lock()
				tasksCount := p.count
				p.tasksLock.Unlock()
				p.environment.Log().Warn(ctx, "delay task pool stopped by timeout", log.Err(ctx.Err()), log.Int("executing_count", tasksCount))
				p.stopTimeoutCounter.Inc(ctx)
			}
		} else {
			p.lock.Unlock()
		}
	})
}
