/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"container/heap"
	"context"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type DelayPool interface {
	Storage
	Delay(deadline time.Duration, fn func()) *DelayTask
}

type DelayTask struct {
	deadline time.Time
	fn       func()
	index    int
}

type PriorityQueue []*DelayTask

func (pq *PriorityQueue) Len() int { return len(*pq) }

func (pq *PriorityQueue) Less(i, j int) bool {
	return (*pq)[i].deadline.Before((*pq)[j].deadline)
}

func (pq *PriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
	(*pq)[i].index = i
	(*pq)[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*DelayTask)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

type DelayPoolImpl struct {
	executorsCount int
	pq             *PriorityQueue
	ch             chan *DelayTask
	wg             sync.WaitGroup
	timer          *time.Timer
	lock           sync.Mutex
	stopCh         chan struct{}
}

func makeDelayPool(executorsCount int) DelayPool {
	return &DelayPoolImpl{
		executorsCount: executorsCount,
		ch:             make(chan *DelayTask),
		pq:             &PriorityQueue{},
	}
}

func (p *DelayPoolImpl) processTimer() {
	p.lock.Lock()
	defer p.lock.Unlock()
	now := time.Now()
	for p.pq.Len() > 0 {
		if !(*p.pq)[0].deadline.After(now) {
			p.ch <- heap.Pop(p.pq).(*DelayTask)
		}
	}
	if p.pq.Len() > 0 {
		p.timer.Reset(time.Until((*p.pq)[0].deadline))
	} else if p.stopCh != nil {
		close(p.stopCh)
	}
}

func (p *DelayPoolImpl) Delay(deadline time.Duration, fn func()) *DelayTask {
	task := &DelayTask{
		deadline: time.Now().Add(deadline),
		fn:       fn,
		index:    -1,
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.pq.Len() == 0 || task.deadline.Before((*p.pq)[0].deadline) {
		if p.timer != nil {
			p.timer.Reset(deadline)
		} else {
			p.timer = time.AfterFunc(deadline, p.processTimer)
		}
	}
	heap.Push(p.pq, task)
	return task
}

func (p *DelayPoolImpl) Start(ctx context.Context) error {
	for i := 0; i < p.executorsCount; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for task := range p.ch {
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
				log.Warnf("delay task pool stopped by timeout and was not empty (tasks count=%d), %s",
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
		close(p.ch)
		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			log.Warnf("delay task pool stopped by timeout: %s", ctx.Err())
		}
	} else {
		p.lock.Unlock()
	}
}
