/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import (
	"context"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/telemetry/metrics"
	log "github.com/sirupsen/logrus"
	"runtime"
	"sync"
)

type Task struct {
	fn   func()
	next *Task
	prev *Task
}

type TaskPool interface {
	Pool
	AddTask(fn func()) *Task
}

type TaskPoolImpl struct {
	head             *Task
	tail             *Task
	lock             sync.Mutex
	name             string
	executorsCount   int
	metrics          metrics.Metrics
	gaugeQueueLength metrics.Gauge
	wg               sync.WaitGroup
	done             bool
	cond             *sync.Cond
	count            int
	config           config.ServiceEnvironment
}

func makeTaskPool(cfg config.ServiceEnvironment, name string, m metrics.Metrics) TaskPool {
	poolConfig := cfg.GetAppConfig().GetPoolByName(name)
	if poolConfig == nil {
		log.Fatalf("task pool %q does not exist.", name)
		return nil
	}

	pool := &TaskPoolImpl{
		name:           name,
		executorsCount: poolConfig.ExecutorsCount,
		metrics:        m,
		config:         cfg,
	}
	if pool.executorsCount == 0 {
		pool.executorsCount = runtime.NumCPU()
	}
	gaugeOpts := metrics.GaugeOpts{
		Opts: metrics.Opts{
			Name: "task_pool_queue_length",
			Help: "Task pool wait queue length",
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

func (p *TaskPoolImpl) AddTask(fn func()) *Task {
	p.lock.Lock()
	defer p.lock.Unlock()
	task := &Task{fn: fn}
	if p.tail != nil {
		task.prev = p.tail
		p.tail.next = task
	} else {
		p.head = task
	}
	p.tail = task
	p.count++
	p.cond.Signal()
	p.gaugeQueueLength.Inc()
	return task
}

func (p *TaskPoolImpl) Start(ctx context.Context) error {
	for i := 0; i < p.executorsCount; i++ {
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
				} else {
					p.head.prev = nil
				}
				task.next = nil
				p.count--
				p.gaugeQueueLength.Dec()
				p.lock.Unlock()
				task.fn()
			}
		}()
	}
	return nil
}

func (p *TaskPoolImpl) Stop(ctx context.Context) {
	p.lock.Lock()
	if p.count > 0 {
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
			log.Warnf("task pool %q stopped by timeout: %s (tasks count=%d)", p.name, ctx.Err(), tasksCount)
		}
	} else {
		p.lock.Unlock()
	}
}
