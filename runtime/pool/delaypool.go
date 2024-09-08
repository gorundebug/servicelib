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
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/telemetry/metrics"
	log "github.com/sirupsen/logrus"
	"runtime"
	"sync"
	"time"
)

type DelayPool interface {
	Pool
	Delay(deadline time.Duration, fn func()) *DelayTask
}

type DelayTask struct {
	deadline time.Time
	fn       func()
	index    int
	next     *DelayTask
	prev     *DelayTask
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
	executorsCount          int
	pq                      *DelayTaskPriorityQueue
	wg                      sync.WaitGroup
	timer                   *time.Timer
	lock                    sync.Mutex
	stopCh                  chan struct{}
	cond                    *sync.Cond
	tasksLock               sync.Mutex
	done                    bool
	stop                    bool
	metrics                 metrics.Metrics
	gaugeWaitQueueLength    metrics.Gauge
	gaugeExecuteQueueLength metrics.Gauge
	head                    *DelayTask
	tail                    *DelayTask
	count                   int
	config                  config.ServiceEnvironmentConfig
}

func makeDelayPool(cfg config.ServiceEnvironmentConfig, m metrics.Metrics) DelayPool {
	pool := &DelayPoolImpl{
		executorsCount: cfg.GetServiceConfig().DelayExecutors,
		pq:             &DelayTaskPriorityQueue{},
		metrics:        m,
		config:         cfg,
	}
	if pool.executorsCount == 0 {
		pool.executorsCount = runtime.NumCPU()
	}
	gaugeOpts := metrics.GaugeOpts{
		Opts: metrics.Opts{
			Name: "delay_pool_wait_queue_length",
			Help: "Delay pool wait queue length",
			ConstLabels: metrics.Labels{
				"service": cfg.GetServiceConfig().Name,
			},
		},
	}
	pool.gaugeWaitQueueLength = m.Gauge(gaugeOpts)
	gaugeOpts = metrics.GaugeOpts{
		Opts: metrics.Opts{
			Name: "delay_pool_execute_queue_length",
			Help: "Delay pool execute queue length",
			ConstLabels: metrics.Labels{
				"service": cfg.GetServiceConfig().Name,
			},
		},
	}
	pool.gaugeExecuteQueueLength = m.Gauge(gaugeOpts)
	pool.cond = sync.NewCond(&pool.tasksLock)
	return pool
}

func (p *DelayPoolImpl) processTimer() {
	p.lock.Lock()
	defer p.lock.Unlock()
	for p.pq.Len() > 0 && !(*p.pq)[0].deadline.After(time.Now()) {
		task := heap.Pop(p.pq).(*DelayTask)
		p.tasksLock.Lock()
		if p.tail != nil {
			task.prev = p.tail
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

func (p *DelayPoolImpl) Delay(deadline time.Duration, fn func()) *DelayTask {
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
	return task
}

func (p *DelayPoolImpl) Start(ctx context.Context) error {
	for i := 0; i < p.executorsCount; i++ {
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
				} else {
					p.head.prev = nil
				}
				task.next = nil
				p.count--
				p.gaugeExecuteQueueLength.Dec()
				p.tasksLock.Unlock()
				task.fn()
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
				log.Warnf("delay task pool stopped by timeout and was not empty (waiting tasks count=%d), %s",
					p.pq.Len(), ctx.Err())
				p.lock.Unlock()
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
			log.Warnf("delay task pool stopped by timeout: %s (executing tasks count=%d)", ctx.Err(), tasksCount)
		}
	} else {
		p.lock.Unlock()
	}
}
