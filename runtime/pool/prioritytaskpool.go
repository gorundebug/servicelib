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
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
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
	pq               *TaskPriorityQueue
	gaugeQueueLength metrics.Gauge
	wg               sync.WaitGroup
	done             bool
	cond             *sync.Cond
	environment      environment.ServiceEnvironment
}

func makePriorityTaskPool(env environment.ServiceEnvironment, name string) PriorityTaskPool {
	poolConfig := env.AppConfig().GetPoolByName(name)
	if poolConfig == nil {
		env.Log().Fatalf("priority task pool %q does not exist.", name)
		return nil
	}
	pool := &PriorityTaskPoolImpl{
		name:        name,
		pq:          &TaskPriorityQueue{},
		environment: env,
	}
	gaugeOpts := metrics.GaugeOpts{
		Opts: metrics.Opts{
			Name: "priority_task_pool_queue_length",
			Help: "Priority task pool wait queue length",
			ConstLabels: metrics.Labels{
				"service": env.ServiceConfig().Name,
				"name":    name,
			},
		},
	}
	m := env.Metrics()
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
	heap.Push(p.pq, task)
	p.lock.Unlock()
	p.gaugeQueueLength.Inc()
	p.cond.Signal()
	return task
}

func (p *PriorityTaskPoolImpl) Start(ctx context.Context) error {
	poolConfig := p.environment.AppConfig().GetPoolByName(p.name)
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
	if p.pq.Len() > 0 {
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
			p.environment.Log().Warnf("priority task pool %q stopped by timeout: %s (tasks count=%d)", p.name, ctx.Err(), tasksCount)
		}
	} else {
		p.lock.Unlock()
	}
}
