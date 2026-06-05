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
	cond                    *sync.Cond
	tasksLock               sync.Mutex
	done                    bool
	stop                    bool
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
	executorsCount := p.environment.ServiceConfig().DelayExecutors
	if executorsCount == 0 {
		executorsCount = runtime.NumCPU()
	}
	for i := 0; i < executorsCount; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				p.tasksLock.Lock()
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
				runTask(p.environment, "delay", task.fn)
				task.fn = nil
				p.tasksTotal.Inc(ctx)
				p.executionDuration.Observe(ctx, time.Since(startTime).Seconds())
			}
		}()
	}
	return nil
}

func (p *DelayPoolImpl) Stop(ctx context.Context) {
	p.lock.Lock()
	if p.pq.Len() > 0 {
		go func() {
			p.stopCh = make(chan struct{})
			p.lock.Unlock()
			select {
			case <-p.stopCh:
			case <-ctx.Done():
				p.lock.Lock()
				p.environment.Log().Warnf("delay task pool stopped by timeout and was not empty (waiting tasks count=%d), %s",
					p.pq.Len(), ctx.Err())
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
			p.environment.Log().Warnf("delay task pool stopped by timeout: %s (executing tasks count=%d)", ctx.Err(), tasksCount)
			p.stopTimeoutCounter.Inc(ctx)
		}
	} else {
		p.lock.Unlock()
	}
}
