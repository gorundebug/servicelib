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
)

type PriorityTask struct {
	fn       func()
	priority int
	index    int
}

type PriorityTaskPool interface {
	Pool
	AddTask(priority int, fn func()) *PriorityTask
}

type PriorityTaskPoolImpl struct {
	lock             sync.Mutex
	name             string
	executorsCount   int
	pq               *TaskPriorityQueue
	metrics          metrics.Metrics
	gaugeQueueLength metrics.Gauge
	wg               sync.WaitGroup
	done             bool
	cond             *sync.Cond
	config           config.ServiceEnvironmentConfig
}

func makePriorityTaskPool(cfg config.ServiceEnvironmentConfig, name string, m metrics.Metrics) PriorityTaskPool {
	poolConfig := cfg.GetConfig().GetPoolByName(name)
	if poolConfig == nil {
		log.Fatalf("priority task pool '%s' does not exist.", name)
		return nil
	}
	pool := &PriorityTaskPoolImpl{
		name:           name,
		executorsCount: poolConfig.ExecutorsCount,
		pq:             &TaskPriorityQueue{},
		metrics:        m,
		config:         cfg,
	}
	if pool.executorsCount == 0 {
		pool.executorsCount = runtime.NumCPU()
	}
	gaugeOpts := metrics.GaugeOpts{
		Opts: metrics.Opts{
			Name: "priority_task_pool_queue_length",
			Help: "Priority task pool wait queue length",
			ConstLabels: metrics.Labels{
				"service": cfg.GetServiceConfig().Name,
				"name":    name,
			},
		},
	}
	pool.gaugeQueueLength = m.Gauge(gaugeOpts)
	pool.cond = sync.NewCond(&pool.lock)
	return pool
}

type TaskPriorityQueue []*PriorityTask

func (pq *TaskPriorityQueue) Len() int { return len(*pq) }

func (pq *TaskPriorityQueue) Less(i, j int) bool {
	return (*pq)[i].priority < (*pq)[j].priority
}

func (pq *TaskPriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
	(*pq)[i].index = i
	(*pq)[j].index = j
}

func (pq *TaskPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*PriorityTask)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *TaskPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (p *PriorityTaskPoolImpl) AddTask(priority int, fn func()) *PriorityTask {
	task := &PriorityTask{
		fn:       fn,
		index:    -1,
		priority: priority,
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	heap.Push(p.pq, task)
	p.gaugeQueueLength.Inc()
	p.cond.Signal()
	return task
}

func (p *PriorityTaskPoolImpl) Start(ctx context.Context) error {
	for i := 0; i < p.executorsCount; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				p.lock.Lock()
				for p.pq.Len() == 0 && !p.done {
					p.cond.Wait()
				}
				if p.pq.Len() == 0 && p.done {
					p.lock.Unlock()
					break
				}
				task := heap.Pop(p.pq).(*PriorityTask)
				p.gaugeQueueLength.Dec()
				p.lock.Unlock()
				task.fn()
			}
		}()
	}
	return nil
}

func (p *PriorityTaskPoolImpl) Stop(ctx context.Context) {
	p.lock.Lock()
	if p.pq.Len() == 0 {
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
			tasksCount := p.pq.Len()
			p.lock.Unlock()
			log.Warnf("priority task pool '%s' stopped by timeout: %s (tasks count=%d)", p.name, ctx.Err(), tasksCount)
		}
	} else {
		p.lock.Unlock()
	}
}
